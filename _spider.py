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

    async def run(self):
        await self._engine.register(self)   # register spider to engine first
        while True:
            task = await self._tasks.get()
            if isinstance(task, Stop):
                logger.info('<spider-{}> quit, {}.'
                            .format(self._name, task.msg))
                break
            logger.info('<spider-{}> got task:<{}>'
                        .format(self._name, task.url))
            try:
                if not task.fetcher:
                    fetcher = self.fetch
                else:
                    fetcher = getattr(self, task.fetcher)
                response = await fetcher(task)
            except AttributeError as e:   # Can't find fether
                raise
            except Exception as e:
                logger.exception(e)
                continue
            else:
                if not response:
                    continue
                elif isinstance(response, Request):
                    await self.send_result(response)
                elif isinstance(response, Response):
                    if not task.parser:
                        parser = self.parse
                    else:
                        parser = getattr(self, task.parser)
                    result = parser(response)
                    if result:
                        await self.send_result(result)
                else:
                    raise TypeError('unkown Type:{},'
                                    'except a Request or Response')
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
