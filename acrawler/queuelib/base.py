import asyncio
import pickle
import logging
from asyncio import QueueEmpty
from collections import deque


class BaseQueue:
    def __init__(self, loop=None):
        self._loop = asyncio.get_event_loop() if not loop else loop
        self._getters = deque()

    def _get(self, count):
        # param count: get count items at once
        raise NotImplementedError

    def _put(self, items) -> bool:
        # param items: items must can be itered
        # return: return True if putted item
        raise NotImplementedError

    async def get(self, count=1):
        assert count > 0
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
        assert count > 0
        if self.empty():
            raise QueueEmpty()
        return self._get(count=count)

    def put_nowait(self, items):
        if not hasattr(items, '__iter__'):
            items = (items, )
        if self._put(items):    # wake up getter, if we had putted item
            self._wakeup_next(self._getters)

    # disktop queue do not has maxsize property
    # so put method is not coroutine
    def put(self, items):
        self.put_nowait(items)

    def _wakeup_next(self, waiters):
        while waiters:
            waiter = waiters.popleft()
            if not waiter.done():
                waiter.set_result(None)
                return

    def empty(self):
        return self.qsize() <= 0

    def qsize(self) -> int:
        # return: count of items in this queue
        raise NotImplementedError

    def close(self):
        raise NotImplementedError

    @staticmethod
    def clean(settings):
        # remove all databases to clean up queue
        raise NotImplementedError


def serialze(item):
    return pickle.dumps(item)


def unserialze(raw_data):
    return pickle.loads(raw_data)
