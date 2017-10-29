import asyncio
import logging
import signal
import pathlib
import functools

from urllib.parse import urlparse

from .scheduler import QueueEmpty
from .model import Stop


logger = logging.getLogger('Engine')


def get_hosts_from_urls(urls):
    hosts = set()
    for url in urls:
        _, host, *x = urlparse(url)
        hosts.add(host)
    return hosts


def exception_handler(loop, context):
    if 'exception' in context:
        logger.exception(context['exception'])
    else:
        logger.warn(context['message'])


class Engine:
    def __init__(self, settings, SpiderClass,
                 SchedulerClass, QueueClass,
                 FilterClass, loop=None, resume=True):
        self._loop = asyncio.get_event_loop() if not loop else loop
        self._first_start_file = '.engine'
        self._settings = settings
        self._SpiderClass = SpiderClass
        self._SchedulerClass = SchedulerClass
        self._QueueClass = QueueClass
        self._FilterClass = FilterClass
        self._scheduler = None
        self._waiters = asyncio.Queue()
        self._spiders = []
        self._tasks = []
        self._threads = self._settings['engine'].get('threads', '1')
        self._engine_coro = None
        self._stop_coro = None
        self._quit_event = asyncio.Event(loop=self._loop)
        self._resume = resume
        self._interrupt = 0
        self._unfinished = 0
        self._initial()

    def _initial(self):
        self._loop.set_exception_handler(exception_handler)
        if not self._resume:
            self._QueueClass.clean(self._settings['scheduler']['queue'])
            self._FilterClass.clean(self._settings['scheduler']['filter'])
            f = pathlib.Path(self._first_start_file)
            if f.is_file():
                f.unlink()
        hosts = set()
        hosts.update(self._SpiderClass.allowed_hosts)
        hosts.update(get_hosts_from_urls(self._SpiderClass.start_urls))
        self._FilterClass.set_hosts(hosts)
        self._scheduler = self._SchedulerClass(
                self._settings.get('scheduler', {}),
                self._FilterClass, self._QueueClass,
                self._loop
        )

    def _signalhandler(self, signame):
        logger.warning('got signal {}'.format(signame))
        self._interrupt += 1
        self.stop()

    def register(self, spider):
        self._waiters.put_nowait(spider)

    def broadcast(self, msg):
        for worker in self._spiders:
            worker.send(msg)

    def _need_boost(self):
        f = pathlib.Path(self._first_start_file)
        if f.is_file():
            return False
        f.touch()
        return True

    def run(self):
        loop = self._loop
        if self._need_boost():
            requests = self._SpiderClass.start_request()
            self.send_result(requests)
        self._stop_coro = loop.create_task(self._wait_stop())
        self._tasks.append(self._stop_coro)
        self._allot_coro = loop.create_task(self._allot())
        self._tasks.append(self._allot_coro)
        for i in range(self._threads):
            spider = self._SpiderClass(
                self, self._settings['spider'], loop=loop
                )
            self._spiders.append(spider)
            self.register(spider)
            self._tasks.append(loop.create_task(spider.run()))
        # add signal handler
        loop.add_signal_handler(
            getattr(signal, 'SIGINT'),
            functools.partial(self._signalhandler, 'SIGINT')
        )
        try:
            loop.run_until_complete(asyncio.wait(self._tasks))
        except Exception as e:
            logger.error('unexpected error ocurred, {}'.format(e))
            return
        finally:
            self.cancel_all()
            self._scheduler.close()
            loop.close()

    def send_result(self, results):
        assert hasattr(results, '__iter__')
        self._scheduler.add(results)

    async def _allot(self):
        # allot task to spider which is idled
        while True:
            waiter = await self._waiters.get()
            try:
                task = await self._scheduler.next(timeout=1)
            except asyncio.TimeoutError:
                if self._unfinished <= 0 and self._scheduler.is_empty():
                    logger.info('all tasks had done')
                    self.stop()
                    break   # tasks is done, break from loop
                self.register(waiter)
                continue
            waiter.send(task)
            self._unfinished += 1

    def task_done(self, spider):
        self.register(spider)
        self._unfinished -= 1

    async def _wait_stop(self):
        await self._quit_event.wait()
        self.broadcast(Stop())
        if self._allot_coro and not self._allot_coro.done():
            self._allot_coro.cancel()

    def cancel_all(self):
        for task in self._tasks:
            if not task.done():
                task.cancel()

    def stop(self):
        if self._interrupt <= 1:
            self._quit_event.set()
        elif self._interrupt >= 5:  # force cancel all coroutines
            self.cancel_all()
