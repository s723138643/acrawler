import logging
import asyncio

from .queuelib import Empty


logger = logging.getLogger('Scheduler')


class QueueEmpty(Exception):
    pass


class Scheduler:
    def __init__(self, settings, FilterClass, QueueClass, loop=None):
        self._loop = asyncio.get_event_loop() if not loop else loop
        self._urlfilter = FilterClass(settings['filter'])
        self.fetchdiskq = QueueClass(settings['queue'], loop=loop)
        self._settings = settings

    async def add(self, request):
        await self._add_request(request)

    async def _add_request(self, request):
        if request.filter_ignore:
            await self.fetchdiskq.put(request)
            logger.debug('add task<{}> ignore filter'.format(request.url))
        else:
            if self._urlfilter.url_allowed(request.url, request.redirect):
                await self.fetchdiskq.put(request)

    def next_nowait(self):
        try:
            req = self.fetchdiskq.get_nowait()
        except Empty:
            raise QueueEmpty()
        return req

    async def next(self, timeout=None):
        if timeout and timeout > 0:
            try:
                t = await asyncio.wait_for(self.fetchdiskq.get(), timeout)
            except asyncio.TimeoutError:
                raise QueueEmpty()
        else:
            t = await self.fetchdiskq.get()
        return t

    def empty(self):
        return self.fetchdiskq.empty()

    def close(self):
        self._urlfilter.close()
        self.fetchdiskq.close()