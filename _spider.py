import asyncio
import logging

logger = logging.getLogger('Spider')


class AbstractSpider:
    def __init__(self, engine, settings, loop=None):
        raise NotImplementedError

    @classmethod
    def start_request(cls):
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
        await self._engine.register(self)
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
            except (IOError, OSError) as e:
                logger.warn('fetch error:{}'.format(e))
                task.filter_ignore = True
                await self._engine.send_result(task)
                continue
            else:
                if not response:
                    continue
                parser = getattr(self, task.parser or '', self.parse)
                result = parser(response)
                if result:
                    await self._engine.send_result(result)
            finally:
                await self._engine.register(self)
        self.close()

    async def send(self, task):
        await self._tasks.put(task)

    async def stop(self):
        await self._tasks.put('quit')
