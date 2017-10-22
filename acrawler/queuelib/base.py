import asyncio
import pickle
import logging
from asyncio import QueueEmpty
from collections import deque


class BaseQueue:
    def __init__(self, loop=None):
        self._loop = asyncio.get_event_loop() if not loop else loop
        self._getters = deque()

    def _get(self, count=1):
        raise NotImplementedError

    def _put(self, items):
        raise NotImplementedError

    async def get(self, count=1):
        while self.empty():
            getter = self._loop.create_future()
            self._getters.append(getter)
            try:
                await getter
            except:
                getter.cancel()
                if not self.empty() and not getter.cancelled():
                    self._wakeup_next(self._getters)
                raise
        return self.get_nowait(count=count)

    def get_nowait(self, count=1):
        if self.empty():
            raise QueueEmpty()
        return self._get(count=count)

    def put(self, items):
        if not hasattr(items, '__iter__'):
            items = (items, )
        if self._put(items) > 0:
            self._wakeup_next(self._getters)

    def _wakeup_next(self, waiters):
        while waiters:
            waiter = waiters.popleft()
            if not waiter.done():
                waiter.set_result(None)
                return

    def empty(self):
        return self.qsize() <= 0

    def qsize(self):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError

    @staticmethod
    def clean(settings):
        raise NotImplementedError


def serialze(item):
    return pickle.dumps(item)


def unserialze(raw_data):
    return pickle.loads(raw_data)
