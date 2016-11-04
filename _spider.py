import asyncio
import logging

from .model import Request


logger = logging.getLogger('Spider')


class AbstractSpider:
    def __init__(self, engine, settings, loop=None):
        raise NotImplementedError

    @classmethod
    def start_request(cls):
        '''bootstrap spider, may called by engine
        '''
        raise NotImplementedError

    async def run(self):
        raise NotImplementedError

    async def send(self, task):
        raise NotImplementedError

    async def stop(self):
        raise NotImplementedError

    async def fetch(self, request):
        raise NotImplementedError

    def parse(self, response):
        raise NotImplementedError


class BaseSpider(AbstractSpider):
    def __init__(self, engine, settings, loop=None):
        self._settings = settings
        self._engine = engine
        self._loop = asyncio.get_event_loop() if not loop else loop
        self._tasks = asyncio.Queue()

    async def run(self):
        await self._engine.register(self)   # register spider to engine first
        while True:
            task = await self._tasks.get()
            if isinstance(task, str) and task == 'quit':
                logger.debug('<spider-{}> quit'.format(self._name))
                break
            logger.debug('<spider-{}> got task:<{}>'
                         .format(self._name, task.url))
            fetcher = getattr(self, task.fetcher or '', self.fetch)
            try:
                response = await fetcher(task)
            except Exception as e:
                logger.warn('fetch error:{}'.format(e))
                continue
            else:
                if not response:
                    continue
                parser = getattr(self, task.parser or '', self.parse)
                result = parser(response)
                if result:
                    await self._send_result(result)
            finally:
                # register spider to engine again
                await self._engine.register(self)
        self.close()

    async def _send_result(self, results):
        # send result to engine
        if not hasattr(results, '__iter__'):
            results = (results, )

        requests = []
        for result in results:
            if result and isinstance(result, Request):
                requests.append(result)
            else:
                logger.debug('func{_send_result} excepted a Request instance')
        await self._engine.send_result(requests)

    async def send(self, task):
        '''send task to spider
        task: a Request instance
        '''
        await self._tasks.put(task)

    async def stop(self):
        # send 'quit' message to spider, but spider do not stop immediately
        await self._tasks.put('quit')
