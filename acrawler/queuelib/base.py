import asyncio
import pickle
import logging

from collections import deque


class Empty(Exception):
    pass


class Full(Exception):
    pass


class BaseQueue:

    def __init__(self, maxsize=0, loop=None):
        self._loop = asyncio.get_event_loop() if not loop else loop
        self._maxsize = maxsize
        self._putters = deque()
        self._getters = deque()

    def _get(self, count=1):
        raise NotImplementedError

    def _put(self, item):
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

    async def put(self, item):
        while self.full():
            putter = self._loop.create_future()
            self._putters.append(putter)
            try:
                await putter
            except:
                putter.cancel()
                if not self.full() and not putter.cancelled():
                    self._wakeup_next(self._putters)
                raise
        return self.put_nowait(item)

    def get_nowait(self, count=1):
        if self.empty():
            raise Empty()
        items = self._get(count=count)
        self._wakeup_next(self._putters)
        return items

    def put_nowait(self, item):
        if self.full():
            raise Full()
        self._put(item)
        self._wakeup_next(self._getters)

    def _wakeup_next(self, waiters):
        if waiters:
            waiter = waiters.popleft()
            if not waiter.done():
                waiter.set_result(None)

    def empty(self):
        return self.qsize() <= 0

    def full(self):
        if self._maxsize <= 0:
            return False
        return self.qsize() >= self._maxsize

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
