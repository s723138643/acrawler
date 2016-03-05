from http.cookiejar import CookieJar

import aiohttp

from .node import FetchNode, WorkNode

Headers = {
        'User-Agent':'Mozilla/5.0 (X11; Linux x86_64; rv:42.0) Gecko/20100101 Firefox/43.0'
        }

class Spider:
    def __init__(self, startURLs=None, headers=None, loop=None):
        self.headers = headers if headers else Headers
        self.cookies = CookieJar()
        self.client = None
        self.loop = loop
        self.startURLs = startURLs
        self.client = aiohttp.ClientSession(headers=self.headers, \
                cookies=self.cookies, loop=self.loop)

    def makeStart(self):
        for i in self.startURLs:
            n = FetchNode(i, callback=self.fetch)
            yield n

    async def fetch(self, n):
        async with self.client.get(n.url) as response:
            body = await response.read()
        n = WorkNode(n.url, body)
        n.callback = self.parse

        return n

    async def parse(self, url):
        raise NotImplementedError()

    def close(self):
        self.client.close()
