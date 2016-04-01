class Node:
    def __init__(
            self,
            url,
            priority=3,
            callback=None,
            args=(),
            kwargs={}):

        self.url = url
        self.priority = priority
        self.callback = callback
        self.args = args
        self.kwargs = kwargs

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

    @classmethod
    def from_callback(cls, callback):
        cls.callback = fetch_fun
        return cls


class Request(Node):

    fetch_fun = None
    def __init__(self, url,
            priority=3,
            redirect=0,
            callback=None,
            args=[],
            kwargs={}):

        super().__init__(url, priority, callback, args, kwargs)
        self.redirect = redirect

    def __str__(self):
        if isinstance(self.callback, str):
            return 'Request<{}, {}>'.format(self.url, self.callback)
        return 'Request<{}, {}>'.format(self.url, self.callback.__name__)

    @classmethod
    def from_fetch_fun(cls, fetch_fun):
        cls.fetch_fun = fetch_fun
        return cls


class Response(Node):
    def __init__(self, url, html,
            priority=3,
            callback=None,
            args=[],
            kwargs={}):

        super().__init__(url, priority, callback, args, kwargs)
        self.html = html

    def __str__(self):
        if isinstance(self.callback, str):
            return 'Request<{}, {}>'.format(self.url, self.callback)
        return 'Response<{}, {}>'.format(self, url, self.callback)
