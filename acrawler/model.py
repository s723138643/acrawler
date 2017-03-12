import json
import time
import logging
import lxml.etree as etree

from pyquery import PyQuery


logger = logging.getLogger('spider')


class Stop:
    def __init__(self, msg=None):
        self.msg = msg


class Request:
    __slots__ = ['url', 'priority', 'fetcher_func', 'parser_func', 'created',
                 'redirect', 'retryed', 'last_activated', 'extra',
                 'filter_ignore']

    def __init__(self, url, *, priority=3, fetcher=None, parser=None,
                 filter_ignore=False):
        self.url = url

        assert isinstance(priority, int), 'priority is not an integer'
        self.priority = priority

        self.redirect = 1
        self.created = time.time()
        self.last_activated = None
        self.retryed = 0
        self.filter_ignore = filter_ignore

        if fetcher and not callable(fetcher):
            raise ValueError('fetcher_func is not callable')
        # async function cannot be serialzed
        self.fetcher_func = fetcher.__name__ if fetcher else None
        if parser and not callable(parser):
            raise ValueError('parser_func is not callable')
        self.parser_func = parser.__name__ if parser else None
        self.extra = None

    @property
    def extra_data(self):
        return self.extra

    @extra_data.setter
    def extra_data(self, extra):
        self.extra = extra

    @property
    def parser(self):
        return self.parser_func

    @parser.setter
    def parser(self, value):
        if not callable(value):
            raise ValueError('{} is not callable'.format(value))
        self.parser_func = value.__name__

    @property
    def fetcher(self):
        return self.fetcher_func

    @fetcher.setter
    def fetcher(self, value):
        if not callable(value):
            raise ValueError('{} is not callable'.format(value))
        self.fetcher_func = value.__name__


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
    def raw(self):
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
