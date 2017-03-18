'''Base spider class, must overwirte by user
'''

import asyncio
import logging

from .model import Request, Response, Stop


logger = logging.getLogger('Spider')


class AbstractSpider:
    """abstract class of spider"""
    start_urls = []
    allowed_hosts = set()

    def __init__(self, engine, settings, loop=None):
        raise NotImplementedError

    @classmethod
    def start_request(cls):
        '''bootstrap spider, called by engine'''
        raise NotImplementedError

    def initialize(self):
        '''initialize spider, may overwrite by user'''
        pass

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

    async def broadcast(self, msg):
        """broadcast msg to all spider(include itself)"""
        pass

    def stop_all(self):
        """stop all spiders"""
        pass

    def close(self):
        '''do nothing by default'''
        pass


class BaseSpider(AbstractSpider):
    def __init__(self, engine, settings, loop=None):
        self._settings = settings
        self._debug = settings['debug']
        self._engine = engine
        self._watchdog = engine._watchdog
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
        if isinstance(response, Request):
            await self.send_result(response)
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
                await self.send_result(result)
        elif isinstance(response, Request):
            await self.send_result(response)
        else:
            raise TypeError('excepted {} object'.format(type(response)))

    async def run(self):
        if asyncio.iscoroutinefunction(self.initialize):
            await self.intialize()
        else:
            self.initialize()
        await self._engine.register(self)   # register spider to engine first
        while True:
            task = await self._tasks.get()
            if isinstance(task, Stop):
                logger.info('recieved stop message, stop now')
                break
            try:
                self._watchdog.feed()       # to indecate spider is processing
                logger.info('got <{}>'.format(task.url))
                response = await self._fetch(task)
                if response:
                    await self._parse(response)
            except asyncio.CancelledError:
                logger.warn('spider was force stoped')
                self.close()
                raise
            except Exception as e:
                if self._debug:
                    logger.exception(e)
                    self.stop_all()
                else:
                    logger.info('processing <{}> failed'.format(task.url))
                    logger.exception(e)
                continue
            finally:
                # to indecate spider was processed
                self._watchdog.consume()
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
                    logger.warn('excepted a Request object')
        await self._engine.send_result(request_filter(results))

    async def send(self, task):
        '''send task to spider, used by engine
        '''
        await self._tasks.put(task)

    async def broadcast(self, msg):
        await self._engine.broadcast(msg)

    def stop_all(self):
        '''stop all spiders, may not stop immediately'''
        self._engine.quit()
