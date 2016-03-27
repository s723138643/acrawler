import logging
import asyncio

from asyncio import PriorityQueue

from .node import Request, Response
from .squeue import PrioritySQLiteQueue, SQLiteEmptyError, SQLiteFullError

class QueueEmpty(Exception):
    pass

class Scheduler:
    def __init__(self, urlfilter, settings, loop=None):
        self.stop = False
        self.loop = loop
        self.urlfilter = urlfilter
        self.settings = settings
        self.maxsize = self.settings.get('max_memq_size')
        self.fetchdiskq = PrioritySQLiteQueue(self.settings.get('sqlite_task_path'))
        self.fetchmemq = PriorityQueue(maxsize=self.maxsize, loop=self.loop)
        self.workq = PriorityQueue(loop=self.loop)

    async def add(self, nodes):
        if hasattr(nodes, '__iter__'):
            for i in nodes:
                await self.add_one(i)
        else:
            await self.add_one(nodes)

    async def add_one(self, node):
        if isinstance(node, Response):
            await self._add_response(node)
        elif isinstance(node, Request):
            await self._add_request(node)

    async def _add_request(self, node):
        if self.urlfilter.url_allowed(node.url, node.redirect):
            if self.fetchdiskq.empty() and not self.fetchmemq.full():
                await self.fetchmemq.put((node, node.priority))
            else:
                await self.fetchdiskq.put(node)
        else:
            logging.debug('request<{}> is not allowed'.format(node.url))

    async def _add_response(self, node):
        await self.workq.put((node, node.priority))

    async def sync(self, timeout=1):
        while not self.stop:
            if self.fetchmemq.qsize() <= 10 and not self.fetchdiskq.empty():
                while True:
                    try:
                        n = self.fetchdiskq.get_nowait()
                        self.fetchmemq.put_nowait(n, n.priority)
                    except:
                        break
            await asyncio.sleep(timeout)

    async def next_request(self):
        try:
            req = self.fetchmemq.get_nowait()
            req = req[0]
        except asyncio.queues.QueueEmpty:
            try:
                req = self.fetchdiskq.get_nowait()
            except SQLiteEmptyError:
                raise QueueEmpty()
        return req

    async def next_response(self):
        n = await self.workq.get()
        return n[0]

    def fetch_queue_empty(self):
        return self.fetchmemq.empty() and self.fetchdiskq.empty()

    def work_queue_empty(self):
        return self.workq.empty()

    def close(self):
        if not self.fetchmemq.empty():
            logging.debug('flush memory queue to disk')
            while True:
                try:
                    x, _ = self.fetchmemq.get_nowait()
                    self.fetchdiskq.put_nowait(x)
                except asyncio.queues.QueueEmpty:
                    break
                except Exception:
                    continue
