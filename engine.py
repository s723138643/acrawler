import asyncio
import logging
import signal
import functools

from .settings import get_settings_from_file

from .scheduler import Scheduler, QueueEmpty
from .sfilter import Filter

class Engine:
    def __init__(self, scheduler, spider, settings, loop):
        self.loop = loop
        self.settings = settings

        self.fetching = True        # feth work flag
        self.working = True         # parse work flag

        self.quit_flag = False

        self.scheduler = scheduler
        self.spider = spider
        self.timeout = 3

        self.fetchThread = self.settings.get('download_threads') or 1
        self.workThread = self.settings.get('parse_threads') or 1
        # activate threads (both fethTread and workThread)
        self.activates = 0

    async def fetch(self, name):
        while self.fetching:
            if self.quit_flag:
                self.fetching = False
                break

            try:
                fetchNode = self.scheduler.next_request()
            except QueueEmpty:
                if self.activates <= 0 and \
                        self.scheduler.work_queue_empty() and \
                        self.scheduler.fetch_queue_empty():
                    self.scheduler.stop = True
                    self.fetching = False
                await asyncio.sleep(0.1)
            except Exception:
                await asyncio.sleep(0.1)
            else:
                if fetchNode:
                    self.activates += 1
                    logging.debug(
                            'FetchThread[{}] fetching: {}'.format(
                                name, fetchNode))
                    call = fetchNode.fetch_fun
#                    print(call)
                    args = fetchNode.args
                    kwargs = fetchNode.kwargs
                    try:
                        if not callable(call):
                            call = getattr(self.spider, call)
                        result = await call(fetchNode, *args, **kwargs)
                    except Exception as e:
                        logging.debug('FetchThread[{}] {}'.format(name, e))
                    else:
                        if result:
                            await self.scheduler.add(result)
                    self.activates -= 1
        logging.debug('FetchThread[{}] quit'.format(name))

    async def work(self, name):
        while self.working:
            try:
                workNode = self.scheduler.next_response()
            except QueueEmpty:
                if self.scheduler.work_queue_empty() and not self.fetching:
                    self.working = False
                await asyncio.sleep(0.1)
            else:
                if workNode:
                    self.activates += 1
                    x = workNode.callback
                    args = workNode.args
                    kwargs = workNode.kwargs
                    try:
                        if not callable(x):
                            x = getattr(self.spider, x)
                        result = x(workNode, *args, **kwargs)
                    except Exception as e:
                        logging.debug('WorkThread[{}] {}'.format(name, e))
                    else:
                        if result:
                            await self.scheduler.add(result)
                    self.activates -= 1
        logging.debug('WorkThread[{}] quit'.format(name))

    def signalhandler(self, signame):
        logging.info('got signal {}: exit'.format(signame))
        self.scheduler.stop = True
        self.quit_flag = True

    def run(self):
        # catch 'Ctrl+C' signal
        for signame in ['SIGINT', 'SIGTERM']:
            self.loop.add_signal_handler(
                    getattr(signal, signame),
                    functools.partial(self.signalhandler, signame))

        if self.scheduler.fetch_queue_empty():
            self.loop.run_until_complete(
                    self.scheduler.add(
                        self.spider.start_request()))
        tasks = []
        for i in range(self.fetchThread):
            tasks.append(asyncio.ensure_future(self.fetch(str(i))))

        for i in range(self.workThread):
            tasks.append(asyncio.ensure_future(self.work(str(i))))

        try:
            self.loop.run_until_complete(asyncio.wait(tasks))
        finally:
            self.scheduler.close()
            self.loop.close()

def execute(
        SpiderClass,
        EngineClass=Engine,
        FilterClass=Filter,
        SchedulerClass=Scheduler,
        config_path='config.json'):

    rootlogging = logging.getLogger()
    rootlogging.setLevel(logging.DEBUG)

    settings = get_settings_from_file(config_path)
    loop = asyncio.get_event_loop()

    spider = SpiderClass(settings=settings, loop=loop)

    urlfilter = Filter(settings=settings)
    urlfilter.hosts = spider.hosts

    scheduler = SchedulerClass(urlfilter, settings, loop)
    engine = Engine(scheduler, spider, settings, loop)

    engine.run()
