import json
import lxml.etree as etree

from pyquery import PyQuery

class Request:
    def __init__(self, url, *,
            priority=3, fetcher=None, parser=None):
        self.url = url
        self.priority = priority
        self.fetcher = fetcher.__name__ if fetcher else None
        self.parser = parser.__name__ if parser else None
        self.filter_ignore = False
        self.redirect = 1

    def __lt__(self, x):
        return self.priority < x.priority

    def __le__(self, x):
        return self.priority <= x.priority

    def __eq__(self, x):
        return self.priority == x.priority

    def __gt__(self, x):
        return self.priority > x.priority

    def __ge__(self, x):
        return self.priority >= x.priority

class Response:
    def __init__(self, url, raw, status=None, headers=None):
        self.url = url
        self._raw = raw
        self._status = status
        self._headers = headers
        self._json = None
        self._xpath = None
        self._css = None

    @property
    def text(self):
        return self._raw

    @property
    def json(self):
        if not self._json:
            self._json =  json.dump(self._raw)
        return self._json

    @property
    def HTML_parser(self):
        if self._xpath is None:
            self._xpath = etree.HTML(self._raw)
        return self._xpath

    @property
    def CSS_parser(self):
        if not self._css:
            self._css = PyQuery(self._raw)
        return self._css

    def xpath(self, patern, *args, **kwargs):
        if self._xpath is None:
            self._xpath = etree.HTML(self._raw)
        return self._xpath.xpath(patern, *args, **kwargs)

    def css(self, patern, *args, **kwargs):
        if not self._css:
            self._css = PyQuery(self._raw)
        return self._css(patern, *args, **kwargs)
