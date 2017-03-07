import aiohttp

from http.cookiejar import CookieJar

from .model import Response
from ._spider import BaseSpider
from ._spider import logger


class Spider(BaseSpider):
    count = 1

    def initialize(self):
        self._closed = False
        self._name = str(self.count)
        self._headers = self._settings.get('headers', None)
        self._cookie = CookieJar()
        self._session = aiohttp.ClientSession(headers=self._headers,
                                                    cookies=self._cookie)
        Spider.count += 1

    async def fetch(self, request, *, decoder=None, **kwargs):
        async with self._session.get(request.url, **kwargs) as response:
            if decoder:
                raw = await response.read()
                text = decoder(raw)
            else:
                text = await response.text()
            status = response.status
            headers = response.headers

        return Response(text, status=status,
                        request=request,
                        headers=headers)

    def parse(self, response):
        raise NotImplementedError

    def close(self):
        self._session.close()
        self._closed = True

    def __del__(self):
        if not self._closed:
            logger.warn('<spider-{}> not closed'.format(self._name))
            self.close()
