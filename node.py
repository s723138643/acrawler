class Node:
    def __init__(self, url, priority=3,
            callback=None, args=[],  kwargs={}):
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


class Request(Node):
    def __init__(self, url, priority=3, redirect=0,
            callback=None, args=[], kwargs={}):
        super().__init__(url, priority, callback, kwargs)
        self.redirect = redirect

class Response(Node):
    def __init__(self, url, html, priority=3,
            callback=None, args=[], kwargs={}):
        super().__init__(url, priority, callback, kwargs)
        self.html = html
