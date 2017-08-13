import aiohttp
from .model import Response
from ._spider import BaseSpider
from ._spider import logger


class Spider(BaseSpider):
    def initialize(self):
        self._closed = False
        self._session = aiohttp.ClientSession(loop=self._loop)

    async def fetch(self, request, **kwargs):
        async with self._session.get(request.url, **kwargs) as response:
            text = await response.text()
            status = response.status
        r = Response(text, status)
        r.request = request
        return r

    def close(self):
        self._closed = True
        if self._session:
            self._session.close()

    def __del__(self):
        if not self._closed:
            self.close()
