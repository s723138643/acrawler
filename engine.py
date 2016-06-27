import asyncio
import logging
import signal
import functools

from .settings import get_settings_from_file

from .scheduler import Scheduler, QueueEmpty
from .sfilter import BlumeFilter as Filter

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

        self.fetchThread = self.settings.get('download_threads')
        self.workThread = self.settings.get('parse_threads')
        # activate threads (fethTread and workThread)
        self.activate_fetch_thread = 0
        self.activate_work_thread = 0
        self.errorlimit = self.settings.get('errorlimit')
        self.error = 0

    async def fetch(self, name):
        async def _do_fetch(request):
            if not request:
                return
            logging.debug(
                    'FetchThread[{}] fetching: {}'.format(
                        name, request))
            fetcher = request.fetch_fun
            args = request.args
            kwargs = request.kwargs
            try:
                if not callable(fetcher):
                    fetcher = getattr(self.spider, fetcher)
                result = await fetcher(request, *args, **kwargs)
            except Exception as e:
                self.error += 1
                logging.error(
                        'FetchThread[{}] {}, count:{}'.format(
                            name, e, self.error))
                if self.errorlimit and self.error > self.errorlimit:
                    logging.error('FetchThread to much error occur, exit')
                    self.fetching = False
                    return
            if result:
                await self.scheduler.add(result)

        while self.fetching:
            if self.quit_flag:
                self.fetching = False
                break

            try:
                fetchNode = self.scheduler.next_request()
            except QueueEmpty:
                if self.scheduler.fetch_queue_empty() \
                        and self.activate_fetch_thread <= 0 \
                        and self.scheduler.work_queue_empty() \
                        and self.activate_work_thread <= 0:
                    self.fetching = False
                else:
                    await asyncio.sleep(0.2)
            except Exception as e:
                logging.error('Error {}'.format(e))
                await asyncio.sleep(0.2)
            else:
                self.activate_fetch_thread += 1
                await _do_fetch(fetchNode)
                self.activate_fetch_thread -= 1
        logging.debug('FetchThread[{}] quit'.format(name))

    async def work(self, name):
        async def _do_work(response):
            if not response:
                return

            callback = response.callback
            args = response.args
            kwargs = response.kwargs
            try:
                if not callable(callback):
                    callback = getattr(self.spider, callback)
                result = callback(response, *args, **kwargs)
            except Exception as e:
                logging.error('WorkThread[{}] error {}'.format(name, e))
                return

            if result:
                await self.scheduler.add(result)

        while self.working:
            try:
                workNode = self.scheduler.next_response()
            except QueueEmpty:
                if self.scheduler.work_queue_empty() \
                        and self.scheduler.fetch_queue_empty() \
                        and self.activate_work_thread <= 0 \
                        and self.activate_fetch_thread <= 0:
                    self.working = False
                else:
                    await asyncio.sleep(0.2)
            else:
                self.activate_work_thread += 1
                await _do_work(workNode)
                self.activate_work_thread -= 1
        logging.debug('WorkThread[{}] quit'.format(name))

    def signalhandler(self, signame):
        logging.warning('got signal {}: exit'.format(signame))
        self.scheduler.stop = True
        self.quit_flag = True

    def run(self):
        # catch 'Ctrl+C' signal
        for signame in ['SIGINT', 'SIGTERM']:
            self.loop.add_signal_handler(
                    getattr(signal, signame),
                    functools.partial(self.signalhandler, signame))

        self.loop.run_until_complete(
                self.scheduler.add(self.spider.start_request())
                )
        tasks = []
        for i in range(self.fetchThread):
            tasks.append(
                    asyncio.ensure_future(
                        self.fetch(str(i))
                        )
                    )

        for i in range(self.workThread):
            tasks.append(
                    asyncio.ensure_future(
                        self.work(str(i))
                        )
                    )

        try:
            self.loop.run_until_complete(
                    asyncio.wait(tasks)
                    )
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
