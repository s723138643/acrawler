from urllib.parse import urlparse
from http.cookiejar import CookieJar

import aiohttp

from .node import Request, Response

class Spider:
    starturls = []
    hosts = set()

    def __init__(self, settings, loop=None):
        self.loop = loop
        self.settings = settings
        self.headers = self.settings.get('crawler_header') or None
        self.cookies = CookieJar()
        self.client = aiohttp.ClientSession(
                headers=self.headers,
                cookies=self.cookies)

    @classmethod
    def from_urls(cls, urls):
        if isinstance(urls, str):
            urls = [urls,]
        hosts = set()
        for i in urls:
            _, host, *x = urlparse(i)
            hosts.add(host)
        cls.starturls = urls
        cls.hosts = hosts
        return cls

    def start_request(self):
        for i in self.starturls:
            n = Request(i, callback=self.fetch)
            yield n

    async def fetch(self, n):
        async with self.client.get(n.url) as response:
            body = await response.read()
        n = Response(n.url, body)
        n.callback = self.parse

        return n

    def parse(self, url):
        raise NotImplementedError()

    def close(self):
        self.client.close()

    def __del__(self):
        self.close()
