'''Base spider class, must overwirte by user
'''

import asyncio
import logging

from .model import Request, Response, Stop


logger = logging.getLogger('Spider')


class AbstractSpider:
    """abstract class of spider"""
    start_urls = []
    hosts = []

    def __init__(self, engine, settings, loop=None):
        raise NotImplementedError

    @classmethod
    def start_request(cls):
        '''bootstrap spider, may called by engine'''
        raise NotImplementedError

    async def run(self):
        """run spider"""
        raise NotImplementedError

    async def send(self, task):
        """send message to this spider"""
        raise NotImplementedError

    async def stop(self):
        """send stop message to this spider"""
        raise NotImplementedError

    async def fetch(self, request):
        """fetch document from website specified by request"""
        raise NotImplementedError

    def parse(self, response):
        """parse document"""
        raise NotImplementedError

    def close(self):
        '''do nothing by default'''
        pass


class BaseSpider(AbstractSpider):
    def __init__(self, engine, settings, loop=None):
        self._settings = settings
        self._engine = engine
        self._loop = asyncio.get_event_loop() if not loop else loop
        self._tasks = asyncio.Queue()

    @classmethod
    def start_request(cls):
        '''bootstrap spider, may called by engine
        '''
        for url in cls.start_urls:
            yield Request(url)

    async def _fetch(self, request):
        if not request.fetcher:
            fetcher = self.fetch
        else:
            fetcher = getattr(self, request.fetcher)
        response = await fetcher(request)
        if response and (not response.request):
            # to make sure raw request in result
            response.request = request
        return response

    async def _parse(self, response):
        def do_parse(r):
            if not r.request.parser:
                parser = self.parse
            else:
                parser = getattr(self, r.request.parser)
            return parser(r)

        if isinstance(response, Response):
            result = do_parse(response)
            if result:
                await self.send_result(result)
        elif isinstance(response, Request):
            await self.send_result(response)
        else:
            raise TypeError('cannot parse {} object'.format(type(response)))

    async def run(self):
        await self._engine.register(self)   # register spider to engine first
        while True:
            task = await self._tasks.get()
            if isinstance(task, Stop):
                logger.info('<spider-{}> quit, {}.'.format(self._name,
                                                           task.msg))
                break

            try:
                logger.info('<spider-{}> got task: {}'.format(self._name,
                                                              task.url))
                response = await self._fetch(task)
                if response:
                    await self._parse(response)
                    logger.info('<spider-{}> task: {} done!'
                                .format(self._name, task.url))
            except AttributeError as e:   # Can't find fether or parser
                raise
            except Exception as e:
                logger.exception(e)
                continue
            finally:
                # register spider to engine again
                await self._engine.register(self)
        self.close()

    async def send_result(self, results):
        # send result to engine
        if not hasattr(results, '__iter__'):
            results = (results, )

        def request_filter(before):
            for result in before:
                if result and isinstance(result, Request):
                    yield result
                else:
                    logger.warn('func{_send_result} excepted '
                                'a Request instance')
        await self._engine.send_result(request_filter(results))

    async def send(self, task):
        '''send task to spider
        task: a Request instance
        '''
        await self._tasks.put(task)

    def stop_all(self):
        '''stop all spiders, may not stop immediately'''
        self._engine.quit()
