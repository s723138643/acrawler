import logging
import asyncio
import json

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
        diskq_path = self.settings.get('sqlite_task_path')
        self.fetchdiskq = PrioritySQLiteQueue(diskq_path, loop=self.loop)
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
            await self.fetchdiskq.put((node, node.priority))

    async def _add_response(self, node):
        await self.workq.put((node, node.priority))

    def next_request(self):
        try:
            req, _ = self.fetchdiskq.get_nowait()
        except SQLiteEmptyError:
            raise QueueEmpty()
        return req

    def next_response(self):
        try:
            n, _ = self.workq.get_nowait()
        except asyncio.QueueEmpty:
            raise QueueEmpty()
        return n

    def fetch_queue_empty(self):
        return self.fetchdiskq.empty()

    def work_queue_empty(self):
        return self.workq.empty()

    def close(self):
        self.urlfilter.close()
        self.fetchdiskq.close()
