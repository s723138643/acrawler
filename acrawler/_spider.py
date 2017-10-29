'''Base spider class, must overwirte by user
'''

import asyncio
import logging

from .model import Request, Response, Stop


logger = logging.getLogger('Spider')


class LogAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        return "{} {}".format(self.extra['name'], msg), kwargs


class BaseSpider:
    start_urls = []
    allowed_hosts = set()
    spider_count = 0

    def __init__(self, engine, settings, loop=None):
        self._loop = asyncio.get_event_loop() if not loop else loop
        self._settings = settings
        self._debug = settings['debug']
        self._engine = engine
        self._tasks = asyncio.Queue()
        BaseSpider.spider_count += 1
        self._name = 'spider-{}'.format(self.spider_count)
        self.log = LogAdapter(logger, {'name': self._name})

    def initialize(self):
        pass

    @classmethod
    def start_request(cls):
        '''bootstrap spider, may called by engine
        '''
        for url in cls.start_urls:
            req = Request(url)
            req.filter_ignore = True
            yield req

    async def _fetch(self, request):
        if not request.fetcher:
            fetcher = self.fetch
        else:
            fetcher = getattr(self, request.fetcher)
        response = await fetcher(request)
        if isinstance(response, Request):
            self.send_result(response)
            return None
        if isinstance(response, Response) and (not response.request):
            # to make sure raw request in result
            response.request = request
        return response

    async def _parse(self, response):
        def do_parse(r):
            if r.request.parser:
                parser = getattr(self, r.request.parser)
            else:
                parser = self.parse
            return parser(r)

        if isinstance(response, Response):
            result = do_parse(response)
            if result:
                self.send_result(result)
        elif isinstance(response, Request):
            self.send_result(response)
        else:
            raise TypeError('unexcepted {} object'.format(type(response)))

    async def run(self):
        # initialize spider
        if asyncio.iscoroutinefunction(self.initialize):
            await self.initialize()
        else:
            self.initialize()
        # start crawlling
        while True:
            task = await self._tasks.get()
            if isinstance(task, Stop):
                remains = []
                # put back remain tasks
                while not self._tasks.empty():
                    task = self._tasks.get_nowait()
                    task.filter_ignore = True
                    remains.append(task)
                if remains:
                    self.send_result(remains)
                self.log.info('recieved stop message, stop now')
                break
            try:
                response = await self._fetch(task)
                if not response:
                    continue
                await self._parse(response)
                self.log.info('task <{}> done'.format(task.url))
            except asyncio.CancelledError:
                self.log.warn('force stoped')
                task.filter_ignore = True
                self.send_result(task)
                break
            except Exception as e:
                task.filter_ignore = True
                self.send_result(task)
                self.log.error('task <{}> failed'.format(task.url))
                if self._debug:
                    self.log.exception(e)
                    self.stop_all()
                continue
            finally:
                # to indecate spider was processed
                self._engine.task_done(self)
        self.close()

    def send_result(self, results):
        ''' send result to engine accept a Request object or a iterable
        object that contains Request object
        '''
        if type(results) in (str, bytes):
            self.log.warn('excepted a Request object')
            return

        def is_request(result):
            if isinstance(result, Request):
                return True
            self.log.warn('excepted a Request object')
            return False

        if not hasattr(results, '__iter__'):
            results = (results, )
        filted = filter(is_request, results)
        self._engine.send_result(filted)

    def send(self, task):
        '''send task to spider, used by engine
        '''
        self._tasks.put_nowait(task)

    def broadcast(self, msg):
        self._engine.broadcast(msg)

    def close(self):
        pass
