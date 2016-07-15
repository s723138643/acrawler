import logging
import asyncio

from .squeue import PrioritySQLiteQueue, SQLiteEmptyError

logger = logging.getLogger('Scheduler')


class QueueEmpty(Exception):
    pass


class Scheduler:
    def __init__(self, urlfilter, settings, loop=None):
        self._loop = asyncio.get_event_loop() if not loop else loop
        self._urlfilter = urlfilter
        self._settings = settings
        self.maxsize = self._settings.get('max_size')
        task_path = self._settings.get('task_path')
        task_name = self._settings.get('task_name')
        self.fetchdiskq = PrioritySQLiteQueue(
                task_path, task_name,
                loop=self._loop)

    async def add(self, requests):
        for request in requests:
            await self._add_request(request)

    async def _add_request(self, request):
        if request.filter_ignore:
            await self.fetchdiskq.put((request, request.priority))
            logger.warn('add request <{}> ignore filter'.format(request.url))
        else:
            if self._urlfilter.url_allowed(request.url, request.redirect):
                await self.fetchdiskq.put((request, request.priority))
            else:
                logger.debug('request is in queue, igonre')

    def next_nowait(self):
        try:
            req, _ = self.fetchdiskq.get_nowait()
        except SQLiteEmptyError:
            raise QueueEmpty()
        return req

    async def next(self):
        req, _ = await self.fetchdiskq.get()
        return req

    def empty(self):
        return self.fetchdiskq.empty()

    def close(self):
        self._urlfilter.close()
        self.fetchdiskq.close()
