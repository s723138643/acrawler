import json
import time
import logging
import lxml.etree as etree

from pyquery import PyQuery


logger = logging.getLogger('spider')


class Request:
    __slots__ = ['url', 'priority', '_fetcher', '_parser', 'created',
                 'redirect', 'retryed', 'last_activated', '_extra',
                 'filter_ignore']

    def __init__(self, url, *, priority=3, fetcher=None, parser=None,
                 filter_ignore=False):
        self.url = url
        self.redirect = 1
        self.created = time.time()
        self.last_activated = None
        self.retryed = 0
        self.filter_ignore = filter_ignore

        assert type(priority) is int, 'priority is not an integer'
        self.priority = priority

        assert callable(fetcher) or fetcher is None, 'fetcher is not callable'
        self._fetcher = fetcher.__name__ if fetcher else None

        assert callable(parser) or parser is None, 'parser is not callable'
        self._parser = parser.__name__ if parser else None

        self._extra = None

    @property
    def extra_data(self):
        return self._extra

    @extra_data.setter
    def extra_data(self, extra):
        self._extra = extra

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


class Response:
    def __init__(self, raw, request=None, status=None, headers=None):
        self._raw = raw
        self._status = status
        self._headers = headers
        self._json = None
        self._xpath = None
        self._css = None
        self.request = request

    @property
    def url(self):
        return self.request.url

    @property
    def response_header(self):
        return self._headers

    @property
    def text(self):
        return self._raw

    @property
    def json(self):
        '''decode raw result as json
        '''
        if not self._json:
            self._json = json.loads(self._raw)
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
