import re
import sqlite3
import asyncio

from pathlib import Path
from collections import deque


from .base import Empty, Full

class SQLiteError(Exception):
    pass


class FifoSQLiteQueue:
    _sql_create = ('CREATE TABLE IF NOT EXISTS queue '
                   '(id INTEGER PRIMARY KEY AUTOINCREMENT,'
                   ' url TEXT,'
                   ' priority INTEGER,'
                   ' _fetcher TEXT,'
                   ' _parser TEXT,'
                   ' filter_ignore INTEGER,'
                   ' redirect INTEGER,'
                   ' created REAL,'
                   ' last_activated REAL,'
                   ' retryed INTEGER)')
    _sql_size = 'SELECT COUNT(*) FROM queue'
    _sql_push = ('INSERT INTO queue '
                 '(url,priority,_fetcher,'
                 ' _parser,filter_ignore,redirect,'
                 ' created,last_activated,retryed) '
                 'VALUES (?,?,?,?,?,?,?,?,?)')
    _sql_pop = 'SELECT * FROM queue ORDER BY id LIMIT 1'
    _sql_del = 'DELETE FROM queue WHERE id = ?'

    def __init__(self, path='.', name='task.db', maxsize=0, loop=None):
        self._closed = False
        self._maxsize = maxsize if maxsize else 0

        self._loop = loop if loop else asyncio.get_event_loop()
        self._path = path if path else '.'
        self._name = name if name else 'task.db'
        self._dbfile = Path(path) / self._name

        self._db = sqlite3.Connection(str(self._dbfile), timeout=60)
        self._db.row_factory = sqlite3.Row
        cursor = self._db.cursor()
        cursor.execute(self._sql_create)

        self._getters = deque()
        self._putters = deque()

    def _put(self, request):
        cursor = self._db.cursor()
        try:
            cursor.execute(self._sql_push,
                           (request.url, request.priority, request.fetcher,
                            request.parser, 1 if request.filter_ignore else 0,
                            request.redirect, request.created,
                            request.last_activated, request.retryed))
        finally:
            cursor.close()

    def _get(self):
        cursor = self._db.cursor()
        try:
            x = cursor.execute(self._sql_pop)
            row = x.fetchone()
            cursor.execute(self._sql_del, (row['id'],))
            result_dict = dict(row)
            del result_dict['id']
        finally:
            cursor.close()

        return result_dict

    def put_nowait(self, request):
        if self.full():
            raise Full('SQLiteQueue is Full')
        self._put(request)
        self._weakup_next(self._getters)

    def get_nowait(self):
        if self.empty():
            raise Empty('SQLiteQueue is empty')
        item = self._get()
        self._weakup_next(self._putters)
        return item

    @property
    def maxsize(self):
        return self._maxsize

    def full(self):
        if self._maxsize <= 0:
            return False
        return self._maxsize <= self.qsize()

    async def put(self, request):
        while self.full():
            putter = self._loop.create_future()
            self._putters.append(putter)
            try:
                await putter
            except:
                putter.cancel()
                if not self.full() and not putter.cancelled():
                    self._weakup_next(self._putters)
                raise
        self.put_nowait(request)

    async def get(self):
        while self.empty():
            getter = self._loop.create_future()
            self._getters.append(getter)
            try:
                await getter
            except:
                getter.cancel()
                if not self.empty() and not getter.cancelled():
                    self._weakup_next(self._getters)
                raise
        return self.get_nowait()

    def _weakup_next(self, waiters):
        if waiters:
            waiter = waiters.popleft()
            if not waiter.done():
                waiter.set_result(None)

    @staticmethod
    def clean(settings):
        dbpath = Path(settings.get('task_path', './'))
        dbfile = dbpath / settings.get('task_name', 'task.db')
        if dbfile.is_file():
            dbfile.unlink()

    def close(self):
        self._closed = True
        size = self.__len__()
        if size > 0:
            self._db.commit()
            self._db.close()
        else:
            self._db.close()
            self.clean()

    def is_closed(self):
        return self._closed

    def qsize(self):
        return self.__len__()

    def empty(self):
        return self.qsize() == 0

    def __len__(self):
        cursor = self._db.cursor()
        x = cursor.execute(self._sql_size)
        return x.fetchone()[0]

    def __del__(self):
        if not self._closed:
            self.close()


class PrioritySQLiteQueue(FifoSQLiteQueue):

    def __init__(self, settings, loop=None):
        self._closed = False
        self._loop = loop if loop else asyncio.get_event_loop()
        self._queues = {}
        self._path = settings.get('task_path', './')
        self._basename = settings.get('task_name', 'task_priority')
        self._maxsize = settings.get('maxsize', 0)
        self._getters = deque()
        self._putters = deque()

        self._pass_or_resume()

    def _pass_or_resume(self):
        task_dir = Path(self._path)
        if not task_dir.is_dir():
            task_dir.mkdir()
        else:
            for child in task_dir.iterdir():
                if child.is_file() and child.match(self._basename+'_*.db'):
                    m = re.search(self._basename+'_(.*)\.db', str(child))
                    if m:
                        priority = m.group(1)
                        queue = FifoSQLiteQueue(
                                self._path,
                                name=child.name, loop=self._loop)
                        self._queues[int(priority)] = queue

    def _create_queue(self, priority, loop=None):
        name = '{}_{}.db'.format(self._basename, priority)
        queue = FifoSQLiteQueue(self._path, name=name, loop=loop)
        self._queues[priority] = queue

        return queue

    def put_nowait(self, items):
        request, priority = items
        if self.full():
            raise Full('SQLiteQueue is full')

        if priority not in self._queues.keys():
            self._create_queue(priority, loop=self._loop)
        queue = self._queues.get(priority)
        if isinstance(queue, FifoSQLiteQueue):
            queue.put_nowait(request)
            self._weakup_next(self._getters)
        else:
            raise SQLiteError('SQLiteQueue error')

    def get_nowait(self):
        if self.empty():
            raise Empty('SQLiteQueue is empty')

        for i in sorted(self._queues.keys()):
            queue = self._queues.get(i)
            if queue:
                try:
                    item = (queue.get_nowait(), i)
                    self._weakup_next(self._putters)
                    return item
                except Empty:
                    continue
        raise Empty('SQLiteQueue is empty')

    async def put(self, items):
        while self.full():
            putter = self._loop.create_future()
            self._putters.append(putter)
            try:
                await putter
            except:
                putter.cancel()
                if not self.full() and not putter.cancelled():
                    self._weakup_next(self._putters)
                raise
        self.put_nowait(items)

    @property
    def _size(self):
        sizes = [i.qsize() for i in self._queues.values()]
        return sum(sizes)

    def qsize(self):
        return self._size

    def __len__(self):
        return self._size

    @staticmethod
    def clean(settings):
        dbpath = Path(settings.get('task_path', './'))
        basename = settings.get('task_name', 'task_priority')
        for f in dbpath.iterdir():
            if f.match(basename+'_*.db'):
                f.unlink()

    def close(self):
        if self._queues:
            for i in self._queues.keys():
                self._queues[i].close()
        self._closed = True

    def is_closed(self):
        return self._closed
