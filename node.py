class Node:
    def __init__(self, url, priority=3, callback=None, kwargs={}):
        self._url = url
        self._priority = priority
        self._callback = callback
        self._kwargs = kwargs

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

    @property
    def url(self):
        return self._url

    @url.setter
    def url(self, value):
        self._url = value

    @property
    def callback(self):
        return self._callback

    @callback.setter
    def callback(self, value):
        self._callback = value

    @property
    def priority(self):
        return self._priority

    @priority.setter
    def priority(self, value):
        self._priority = value

    @property
    def kwargs(self):
        return self._kwargs

    @kwargs.setter
    def kwargs(self, value):
        self._kwargs = value

class FetchNode(Node):
    def __init__(self, url, referer=None, method='GET', \
            priority=3, data=None, callback=None, kwargs={}):
        Node.__init__(self, url, priority, callback, kwargs)
        self._referer = referer
        self._method = method
        self._data = data

    @property
    def data(self):
        return self._data

    @data.setter
    def data(self, value):
        self._data = value

    @property
    def method(self):
        return self._method

    @method.setter
    def method(self, value):
        self._method = value

    @property
    def referer(self):
        return self._referer

    @referer.setter
    def referer(self, value):
        self._referer = value

class WorkNode(Node):
    def __init__(self, url, html, priority=3, callback=None, kwargs={}):
        Node.__init__(self, url, priority, callback, kwargs)
        self._html = html

    @property
    def html(self):
        return self._html

    @html.setter
    def html(self, value):
        self._html = value
