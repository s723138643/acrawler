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
            logger.warn('add request <{}> ignore filter'.format(request.url))
        else:
            if self._urlfilter.url_allowed(request.url, request.redirect):
                await self.fetchdiskq.put(request)
            else:
                logger.debug('request is in queue, igonre')

    def next_nowait(self):
        try:
            req = self.fetchdiskq.get_nowait()
        except Empty:
            raise QueueEmpty()
        return req

    async def next(self):
        req = await self.fetchdiskq.get()
        return req

    def empty(self):
        return self.fetchdiskq.empty()

    def close(self):
        self._urlfilter.close()
        self.fetchdiskq.close()
