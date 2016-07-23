import json
import time
import lxml.etree as etree

from pyquery import PyQuery


class Request:

    def __init__(self, url, *,
                 priority=3, fetcher=None, parser=None,
                 filter_ignore=False):
        self.url = url
        assert type(priority) is int, 'priority not an integer'
        self.priority = priority
        assert callable(fetcher) or fetcher is None, 'fetcher not callable'
        self._fetcher = fetcher.__name__ if fetcher else None
        assert callable(parser) or parser is None, 'parser not callable'
        self._parser = parser.__name__ if parser else None
        self.filter_ignore = filter_ignore
        self.redirect = 1
        self.created = time.time()
        self.last_activated = None
        self.retryed = 0

    def to_dict(self):
        tmp = self.__dict__.copy()
        return tmp

    def to_json(self):
        return json.dumps(self.to_dict())

    @property
    def parser(self):
        return self._parser

    @parser.setter
    def parser(self, value):
        if callable(value):
            self._parser = value.__name__
        else:
            raise ValueError('{} is not callable'.format(value))

    @property
    def fetcher(self):
        return self._fetcher

    @fetcher.setter
    def fetcher(self, value):
        if callable(value):
            self._fetcher = value.__name__
        else:
            raise ValueError('{} is not callable'.format(value))

    @staticmethod
    def from_json(text):
        __dict = json.loads(text)
        return Request.from_dict(__dict)

    @staticmethod
    def from_dict(__dict):
        instance = Request(__dict['url'], priority=__dict['priority'],
                           filter_ignore=__dict['filter_ignore'])
        instance.__dict__['_fetcher'] = __dict['_fetcher']
        instance.__dict__['_parser'] = __dict['_parser']
        instance.redirect = __dict['redirect']
        instance.created = __dict['created']
        instance.last_activated = __dict['last_activated']
        instance.retryed = __dict['retryed']

        return instance


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
            self._json = json.dump(self._raw)
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
