import asyncio
import logging
import signal
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
                 FilterClass, loop=None):
        self._loop = asyncio.get_event_loop() if not loop else loop
        self._settings = settings.get('engine', {})
        hosts = get_hosts_from_urls(SpiderClass.start_urls)
        FilterClass.set_hosts(hosts)
        self._scheduler = SchedulerClass(settings.get('scheduler', {}),
                                         FilterClass, QueueClass,
                                         self._loop)
        self._SpiderClass = SpiderClass
        self._spider_settings = settings.get('spider', {})

        self._waiters = asyncio.Queue()
        self._spiders = []
        self._tasks = []

        self._engine = None
        self._stop = None
        self._quit = asyncio.Event(loop=self._loop)

    def signalhandler(self, signame):
        logger.warning('got signal {}'.format(signame))
        self.quit()

    async def register(self, spider):
        await self._waiters.put(spider)

    async def broadcast(self, msg):
        for worker in self._spiders:
            await worker.send(msg)

    def run(self, resume=True):
        if not resume:
            requests = self._SpiderClass.start_request()
            self._loop.run_until_complete(self._scheduler.add(requests))
        for i in range(self._settings['threads']):
            spider = self._SpiderClass(
                    self, self._spider_settings, self._loop)
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
        if not hasattr(result, '__iter__'):
            result = (result,)
        await self._scheduler.add(result)

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
