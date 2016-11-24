from collections import deque

from .base import BaseQueue


class PriorityQueue(BaseQueue):

    def __init__(self, settings, maxsize=0, loop=None):
        super().__init__(maxsize=maxsize, loop=loop)
        self._settings = settings
        self._collections = deque()

    def _put(self, item):
        self._collections.append(item)

    def _get(self):
        return self._collections.popleft()

    def close(self):
        self._collections.clear()

    def qsize(self):
        return len(self._collections)

    @staticmethod
    def clean(settings):
        pass
