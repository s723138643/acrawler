import asyncio
import logging
import signal
import pathlib
import functools

from urllib.parse import urlparse

from .scheduler import QueueEmpty

logger = logging.getLogger('Engine')


def get_hosts_from_urls(urls):
    hosts = []
    for url in urls:
        _, host, *x = urlparse(url)
        hosts.append(host)
    return set(hosts)


class Engine:
    def __init__(self, settings, SpiderClass,
                 SchedulerClass, QueueClass,
                 FilterClass, loop=None, resume=True):
        self._loop = asyncio.get_event_loop() if not loop else loop
        self._flag = '.engine'
        self._settings = settings
        self._SpiderClass = SpiderClass
        self._SchedulerClass = SchedulerClass
        self._QueueClass = QueueClass
        self._FilterClass = FilterClass

        self._scheduler = None
        self._hosts = None

        self._waiters = asyncio.Queue()
        self._spiders = []
        self._tasks = []

        self._threads = self._settings['engine'].get('threads', '1')
        self._engine = None
        self._stop = None
        self._quit = asyncio.Event(loop=self._loop)
        self._resume = resume

        self._initial()

    def _initial(self):
        if not self._resume:
            self._QueueClass.clean(self._settings['scheduler']['queue'])
            self._FilterClass.clean(self._settings['scheduler']['filter'])
            f = pathlib.Path(self._flag)
            if f.is_file():
                f.unlink()
        self._hosts = get_hosts_from_urls(self._SpiderClass.start_urls)
        self._FilterClass.set_hosts(self._hosts)
        self._scheduler = self._SchedulerClass(
                self._settings.get('scheduler', {}),
                self._FilterClass, self._QueueClass,
                self._loop)

    def signalhandler(self, signame):
        logger.warning('got signal {}'.format(signame))
        self.quit()

    async def register(self, spider):
        await self._waiters.put(spider)

    async def broadcast(self, msg):
        for worker in self._spiders:
            await worker.send(msg)

    def need_boost(self):
        f = pathlib.Path(self._flag)
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
            spider = self._SpiderClass(self, self._settings['spider'],
                                       self._loop)
            self._spiders.append(spider)
            self._tasks.append(asyncio.ensure_future(spider.run()))
        self._engine = asyncio.ensure_future(self.engine())
        self._tasks.append(self._engine)
        self._stop = asyncio.ensure_future(self.stop())
        self._tasks.append(self._stop)
        # add signal handler
        self._loop.add_signal_handler(getattr(signal, 'SIGINT'),
                                      functools.partial(self.signalhandler,
                                                        'SIGINT'))
        try:
            self._loop.run_until_complete(asyncio.wait(self._tasks))
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
            try:
                task = self._scheduler.next_nowait()
            except QueueEmpty:
                if self._waiters.qsize() == len(self._spiders):
                    logger.debug('tasks were done, quit...')
                    await self.broadcast('quit')
                    self._stop.cancel()
                    break   # tasks is done, break from loop
                else:
                    await asyncio.sleep(0.2)
                    continue
            else:
                waiter = await self._waiters.get()
                await waiter.send(task)
        logger.debug('engine quit')

    async def stop(self):
        await self._quit.wait()
        logger.debug("send 'quit' message to spiders")
        await self.broadcast('quit')
        if self._engine:
            self._engine.cancel()

    def quit(self):
        self._quit.set()
