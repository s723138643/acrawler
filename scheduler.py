import logging
import asyncio

from .node import FetchNode, WorkNode
from .squeue import PrioritySQLiteQueue, Empty, Full

class Scheduler:
    def __init__(self, spider, urlfilter=None, timeout=1):
        self.loop = asyncio.get_event_loop()
        self.spider = spider
        self.urlfilter = None if not urlfilter else urlfilter
        self.fetchQueue = PrioritySQLiteQueue('task.db')
        self.workQueue = asyncio.PriorityQueue(loop=self.loop)
        self.fetching = True
        self.timeout = timeout

    def makeStart(self):
       nodes = self.spider.makeStart()
       self.addToQueue(nodes)

    async def fetch(self, retry=3):
        while self.fetching:
            try:
                fetchNode = await self.fetchQueue.get(timeout=self.timeout)
            except asyncio.TimeoutError:
                if self.fetchQueue.empty() and self.workQueue.empty():
                    self.fetching = False
                else:
                    continue
            else:
                if fetchNode and isinstance(fetchNode, FetchNode):
                    x = fetchNode.callback
                    kwargs = fetchNode.kwargs
                    try:
                        call = getattr(self.spider, x)
                        result = await call(fetchNode, **kwargs)
                    except Exception as e:
                        logging.debug('[Scheduler] fetchThread error: {}'.format(e))
                    else:
                        if result:
                            self.addToQueue(result)
        logging.debug('[Scheduler] fetch url complete, quit')
        print('fetch thread quit')

    async def work(self):
        while self.fetching:
            try:
                workNode = await asyncio.wait_for(
                    self.workQueue.get(), self.timeout, loop=self.loop)
            except asyncio.TimeoutError:
                continue
            else:
                if workNode and isinstance(workNode, WorkNode):
                    x = workNode.callback
                    kwargs = workNode.kwargs
                    try:
                        result = x(workNode, **kwargs)
                    except Exception as e:
                        logging.debug(
                            '[Scheduler] workThread error: {}'.format(e))
                    else:
                        if result:
                            self.addToQueue(result)
                self.workQueue.task_done()
        print('work thread quit')

    def addToFetchQueue(self, node):
        if self.urlfilter and (node.method=='GET' or node.method=='POST'):
            if not self.urlfilter.inDB(node.url):
                self.urlfilter.addToDB(node.url)
                self.fetchQueue.put_nowait(node)
        else:
            self.fetchQueue.put_nowait(node)

    def addToQueue(self, result):
        if result and hasattr(result, '__iter__'):
            for i in result:
                if isinstance(i, FetchNode):
                    self.addToFetchQueue(i)
                elif isinstance(i, WorkNode):
                    self.workQueue.put_nowait(i)
        elif isinstance(result, FetchNode):
            self.addToFetchQueue(result)
        elif isinstance(result, WorkNode):
            self.workQueue.put_nowait(result)
        print('fetchQueue size:{}, workQueue size:{}'
              .format(self.fetchQueue.qsize(), self.workQueue.qsize()))

    def run(self, fetchThreads=1, workThreads=1, timeout=1):
        if self.fetchQueue.empty():
            self.makeStart()

        tasks = []
        for i in range(fetchThreads):
            tasks.append(asyncio.ensure_future(self.fetch(3)))
        for i in range(workThreads):
            tasks.append(asyncio.ensure_future(self.work()))

        self.loop.run_until_complete(asyncio.wait(tasks))

        self.spider.close()
        self.loop.close()
        self.fetchQueue.close()
