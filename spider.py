import aiohttp

from http.cookiejar import CookieJar

from .model import Response
from ._spider import BaseSpider
from ._spider import logger as logger


class Spider(BaseSpider):
    count = 1

    def __init__(self, engine, settings, loop=None):
        super(Spider, self).__init__(engine, settings, loop)

    def _initialize(self):
        self._name = str(self.count)
        self._headers = self._settings.get('headers', None)
        self._cookie = CookieJar()
        self._session = aiohttp.ClientSession(headers=self._headers,
                                              cookies=self._cookie)
        self._closed = False
        Spider.count += 1

    async def fetch(self, request, decoder=None):
        async with self._session.get(request.url) as response:
            if not decoder:
                text = await response.text()
            else:
                raw = await response.read()
                text = decoder(raw)
            status = response.status
            headers = response.headers

        return Response(text, status=status, request=request,
                        headers=headers)

    def parse(self, response):
        raise NotImplementedError

    def close(self):
        self._session.close()
        self._closed = True

    def __del__(self):
        if not self._closed:
            logger.warn('<spider-{}> unclosed'.format(self._name))
            self.close()
