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


class WathDog:
    def __init__(self, maxsize):
        self._bucket = asyncio.Queue(maxsize=maxsize)

    def feed(self):
        self._bucket.put_nowait(1)

    def consume(self):
        self._bucket.get_nowait()

    def empty(self):
        return self._bucket.empty()


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
        self._watchdog = WathDog(self._threads)
        self._engine = None
        self._stop = None
        self._quit = asyncio.Event(loop=self._loop)
        self._resume = resume
        self._interrupt = 0

        self._initial()

    def _initial(self):
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

    def signalhandler(self, signame):
        logger.warning('got signal {}'.format(signame))
        self._interrupt += 1
        self.quit()

    async def register(self, spider):
        await self._waiters.put(spider)

    async def broadcast(self, msg):
        for worker in self._spiders:
            await worker.send(msg)

    def need_boost(self):
        f = pathlib.Path(self._first_start_file)
        if f.is_file():
            return False
        else:
            f.touch()
            return True

    def run(self):
        if self.need_boost():
            requests = self._SpiderClass.start_request()
            self._loop.run_until_complete(self.send_result(requests))
        for i in range(self._threads):
            spider = self._SpiderClass(self,
                                       self._settings['spider'],
                                       self._loop)
            self._spiders.append(spider)
            self._tasks.append(asyncio.ensure_future(spider.run()))
        self._engine = asyncio.ensure_future(self.engine())
        self._tasks.append(self._engine)
        self._stop = asyncio.ensure_future(self.wait_stop())
        self._tasks.append(self._stop)
        # add signal handler
        self._loop.add_signal_handler(
            getattr(signal, 'SIGINT'),
            functools.partial(self.signalhandler, 'SIGINT')
        )
        try:
            self._loop.run_until_complete(asyncio.wait(self._tasks))
        except Exception as e:
            logger.error('unexpected error ocurred, {}'.format(e))
        finally:
            self._scheduler.close()
            self._loop.close()

    async def send_result(self, result):
        if not result:
            logger.debug('add None value to Scheduler')
        else:
            if not hasattr(result, '__iter__'):
                result = (result, )
            for r in result:
                await self._scheduler.add(r)

    async def engine(self):
        while True:
            waiter = await self._waiters.get()
            try:
                task = await self._scheduler.next(timeout=1)
            except QueueEmpty:
                if self._watchdog.empty():
                    logger.info('all tasks had done')
                    self.quit()
                    break   # tasks is done, break from loop
                else:
                    await self._waiters.put(waiter)
                    continue
            except asyncio.CancelledError:
                raise
            else:
                await waiter.send(task)

    async def wait_stop(self):
        def cancel_all():
            if self._engine and not self._engine.cancelled():
                self._engine.cancel()
            for spider in self._spiders:
                if not spider.cancelled():
                    spider.cancel()

        try:
            await self._quit.wait()
        except asyncio.CancelledError:
            logger.warn('force stop message recieved, shutdown now')
            cancel_all()
            raise
        else:
            if self._engine and not self._engine.cancelled():
                self._engine.cancel()
            await self.broadcast(Stop())

    def quit(self):
        if self._interrupt <= 1:
            self._quit.set()
        elif self._interrupt >= 5:  # force cancel all coroutines
            self._stop.cancel()
