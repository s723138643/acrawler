from collections import deque, defaultdict

from .base import BaseQueue


class PriorityQueue(BaseQueue):

    def __init__(self, settings, maxsize=0, loop=None):
        super().__init__(maxsize=maxsize, loop=loop)
        self._settings = settings
        self._prioritys = dict()
        self._total = 0

    def _put(self, items):
        for item in items:
            priority = item.priority
            if priority in self._prioritys:
                queue = self._prioritys[priority]
            else:
                queue = deque()
                self._prioritys[priority] = queue
            queue.append(item)
            self._total += 1

    def _get(self, count=1):
        remains = count
        items = []
        for priority in sorted(self._prioritys.keys()):
            if remains <= 0:
                break
            queue = self._prioritys[priority]
            while queue and remains > 0:
                items.append(queue.popleft())
                self._total -= 1
                remains -= 1
        if not items:
            raise QueueEmpty()
        return items

    def close(self):
        self._total = 0
        for queue in self._prioritys.values():
            queue.clear()

    def qsize(self):
        return self._total

    @staticmethod
    def clean(settings):
        pass
