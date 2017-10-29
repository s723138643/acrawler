import logging
import asyncio

from collections import deque
from .queuelib.base import QueueEmpty


logger = logging.getLogger('Scheduler')


class Scheduler:
    def __init__(self, settings, FilterClass, QueueClass, loop=None):
        self._loop = asyncio.get_event_loop() if not loop else loop
        self._urlfilter = FilterClass(settings['filter'])
        self.fetchdiskq = QueueClass(settings['queue'], loop=loop)
        self._cache_size = 10
        self._settings = settings
        self._caches = deque()  # use memory cache to reduce disk IO

    def add(self, requests):
        filted = filter(self._should_add, requests)
        self.fetchdiskq.put(filted)

    def _should_add(self, request):
        if request.filter_ignore:
            logger.debug('add task<{}> ignore filter'.format(request.url))
            return True
        if self._urlfilter.allowed(request):
            return True
        logger.debug('task<{}> is not allowed'.format(request.url))
        return False

    def next_nowait(self):
        if not self._caches:
            tasks = self.fetchdiskq.get_nowait(self._cache_size)
            self._caches.extend(tasks)
        return self._caches.popleft()

    async def next(self, timeout=None):
        if timeout:
            assert timeout > 0
        if not self._caches:
            tasks = await self._get(self._cache_size, timeout=timeout)
            self._caches.extend(tasks)
        return self._caches.popleft()

    async def _get(self, count=10, timeout=None):
        if timeout:
            coroutin = self.fetchdiskq.get(count)
            tasks = await asyncio.wait_for(coroutin, timeout)
        else:
            tasks = await self.fetchdiskq.get(count)
        return tasks

    def is_empty(self):
        return (not self._caches) and self.fetchdiskq.empty()

    def close(self):
        self._urlfilter.close()
        self.fetchdiskq.close()
