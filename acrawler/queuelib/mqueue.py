from collections import deque

from .base import BaseQueue


class PriorityQueue(BaseQueue):

    def __init__(self, settings, maxsize=0, loop=None):
        super().__init__(maxsize=maxsize, loop=loop)
        self._settings = settings
        self._prioritys = dict()
        self._total = 0

    def _put(self, item):
        priority = item.priority
        if priority in self._prioritys:
            queue = self._prioritys[priority]
        else:
            queue = deque()
            self._prioritys[priority] = queue
        queue.append(item)
        self._total += 1

    def _get(self, count=1):
        for priority in sorted(self._prioritys.keys()):
            queue = self._prioritys[priority]
            if queue:
                self._total -= 1
                return [queue.popleft()]
        raise Empty

    def close(self):
        self._total = 0
        for queue in self._prioritys.values():
            queue.clear()

    def qsize(self):
        return self._total

    @staticmethod
    def clean(settings):
        pass
