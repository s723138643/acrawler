import asyncio
import aiohttp
import logging

from http.cookiejar import CookieJar

from .model import Request, Response

logger = logging.getLogger('Spider')


class Spider:
    count = 1

    def __init__(self, engine, settings, loop=None):
        self._settings = settings
        self._loop = asyncio.get_event_loop() if not loop else loop
        self._engine = engine
        self._tasks = asyncio.Queue()
        self._name = str(self.count)
        self._headers = self._settings.get('headers', None)
        self._cookie = CookieJar()
        self._session = aiohttp.ClientSession(headers=self._headers,
                                              cookies=self._cookie)
        self._closed = False
        self._encoding = None
        Spider.count += 1

    @classmethod
    def start_request(cls):
        requests = []
        for url in cls.start_urls:
            requests.append(Request(url))
        return requests

    async def run(self):
        await self._engine.register(self)
        while True:
            task = await self._tasks.get()
            if isinstance(task, str) and task == 'quit':
                logger.debug('<spider-{}> quit'.format(self._name))
                break
            logger.debug('<spider-{}> got task:<{}>'.format(self._name,
                                                            task.url))
            fetcher = getattr(self, task.fetcher or '', self.fetch)
            try:
                response = await fetcher(task)
            except Exception as e:
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

    async def fetch(self, request):
        async with self._session.get(request.url) as response:
            html = await response.text()
            status = response.status
            headers = response.headers

        return Response(request.url, html, status=status, headers=headers)

    def parse(self, response):
        raise NotImplementedError

    def close(self):
        self._session.close()
        self._closed = True

    def __del__(self):
        if not self._closed:
            logger.warn('<spider-{}> unclosed'.format(self._name))
            self.close()
