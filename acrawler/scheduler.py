import logging
import asyncio

from collections import deque
from .queuelib import Empty


logger = logging.getLogger('Scheduler')
dozen = 10


class QueueEmpty(Exception):
    pass


class Scheduler:
    def __init__(self, settings, FilterClass, QueueClass, loop=None):
        self._loop = asyncio.get_event_loop() if not loop else loop
        self._urlfilter = FilterClass(settings['filter'])
        self.fetchdiskq = QueueClass(settings['queue'], loop=loop)
        self._settings = settings
        self._caches = deque()

    async def add(self, request):
        await self._add_request(request)

    async def _add_request(self, request):
        if request.filter_ignore:
            await self.fetchdiskq.put(request)
            logger.debug('add task<{}> ignore filter'.format(request.url))
        else:
            if self._urlfilter.allowed(request):
                await self.fetchdiskq.put(request)

    def next_nowait(self):
        if not self._caches:
            try:
                tasks = self.fetchdiskq.get_nowait(dozen)
            except Empty:
                raise QueueEmpty()
            self._caches.extend(tasks)
        return self._caches.popleft()

    async def next(self, timeout=None):
        if not self._caches:
            if timeout and timeout > 0:
                for i in range(3):  # retry 3 times if queue is not empty
                    coroutin = self.fetchdiskq.get(dozen)
                    try:
                        tasks = await asyncio.wait_for(coroutin, timeout)
                    except asyncio.TimeoutError:
                        if self.fetchdiskq.empty():
                            raise QueueEmpty()
                        continue
                    break
                else:
                    raise Exception('got task stalled, but queue is not empty')
            else:
                tasks = await self.fetchdiskq.get(dozen)
            self._caches.extend(tasks)
        return self._caches.popleft()

    def is_done(self):
        return self.fetchdiskq.empty()

    def close(self):
        self._urlfilter.close()
        self.fetchdiskq.close()
