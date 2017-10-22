import json
import time
import datetime
import logging


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
        # coroutine cannot be serialzed
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

    def __str__(self):
        return 'Request<{}, priority={}>'.format(self.url, self.priority)


class Response:
    def __init__(self, raw, request=None, status=None, headers=None):
        self._raw = raw
        self._status = status
        self._headers = headers
        self._json = None
        self.date = datetime.datetime.now()
        self.request = request

    @property
    def url(self):
        return self.request.url

    @property
    def text(self):
        return self._raw

    @property
    def raw(self):
        return self._raw

    def __str__(self):
        return 'Response<{}>'.format(self.url)
