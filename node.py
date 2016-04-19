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
        self.filter_ignore = False

    def __str__(self):
        if callable(self.callback):
            return 'Request<{} [{}] {}>'.format(
                    self.url, self.priority, self.callback.__name__)
        return 'Request<{} [{}] {}>'.format(
                self.url, self.priority, self.callback)

    @classmethod
    def set_fetcher(cls, fetch_fun):
        cls.fetch_fun = fetch_fun
        return cls

    @classmethod
    def from_callback(cls, callback, priority=3, *args, **kwargs):
        return cls(callback, priority, args, kwargs)

class Response(Node):
    def __init__(self, url, html,
            priority=3,
            callback=None,
            args=(),
            kwargs={}):

        super().__init__(url, priority, callback, args, kwargs)
        self.html = html

    def __str__(self):
        if callable(self.callback):
            return 'Response<{} [{}] {}>'.format(
                    self.url, self.priority, self.callback.__name__)
        return 'Response<{} [{}] {}>'.format(
                self.url, self.priority, self.callback)
