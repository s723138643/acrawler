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

    def _get(self):
        raise NotImplementedError

    def _put(self, items):
        raise NotImplementedError

    async def get(self):
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
        return self.get_nowait()

    async def put(self, items):
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
        return self.put_nowait(items)

    def get_nowait(self):
        if self.empty():
            raise Empty
        item = self._get()
        self._wakeup_next(self._putters)
        return item

    def put_nowait(self, items):
        if self.full():
            raise Full
        self._put(items)
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
