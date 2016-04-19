import json
import sqlite3
import pickle
import time
import asyncio
import logging

from pathlib import Path


class SQLiteEmptyError(Exception):
    pass

class SQLiteFullError(Exception):
    pass

class SQLiteError(Exception):
    pass


class FifoSQLiteQueue:

    _sql_create = (
        'CREATE TABLE IF NOT EXISTS queue '
        '(id INTEGER PRIMARY KEY AUTOINCREMENT, item BLOB)'
    )
    _sql_size = 'SELECT COUNT(*) FROM queue'
    _sql_push = 'INSERT INTO queue (item) VALUES (?)'
    _sql_pop = 'SELECT id, item FROM queue ORDER BY id LIMIT 1'
    _sql_del = 'DELETE FROM queue WHERE id = ?'

    def __init__(self, path='.', name='task.db', maxsize=0, loop=None):
        self._maxsize = maxsize if maxsize else 0

        self._loop = loop if loop else asyncio.get_event_loop()
        self._path = path if path else '.'
        self._name = name if name else 'task.db'
        dbfile = Path(path) / self._name

        self._db = sqlite3.Connection(str(dbfile), timeout=60)
        self._db.text_factory = bytes
        self._cursor = self._db.cursor()
        self._cursor.execute(self._sql_create)

    def put_nowait(self, item):
        if self.full():
            raise SQLiteFullError('SQLiteQueue is Full')
        self._cursor.execute(self._sql_push, (item,))

    def get_nowait(self):
        if self.empty():
            raise SQLiteEmptyError('SQLiteQueue is empty')

        x = self._cursor.execute(self._sql_pop)
        _id, _item = x.fetchone()
        self._cursor.execute(self._sql_del, (_id,))

        return _item

    @property
    def maxsize(self):
        return self._maxsize

    def full(self):
        if self._maxsize <= 0:
            return False
        return self._maxsize <= self.qsize()

    async def put(self, item, block=True, timeout=None):
        if not block:
            self.put_nowait(item)
            return

        if timeout and timeout < 0.0:
            raise ValueError("'timeout' must be a non-negative number")

        start = time.time()
        while True:
            try:
                self.put_nowait(item)
            except SQLiteFullError:
                await asyncio.sleep(0.1, loop=self._loop)
            else:
                break
            if timeout and time.time() - start > timeout:
                if self.full():
                    raise SQLiteFullError('SQLiteQueue is full')
                raise asyncio.TimeoutError('SQLiteQueue put method timeout')

    async def get(self, block=True, timeout=None):
        if not block:
            return self.get_nowait()

        if timeout and timeout < 0.0:
            raise ValueError("'timeout' must be a non-negative number")

        start = time.time()
        while True:
            try:
                return self.get_nowait()
            except SQLiteEmptyError:
                await asyncio.sleep(0.1, loop=self._loop)
            if timeout and time.time() - start >= timeout:
                if self.empty():
                    raise SQLiteEmptyError('SQLiteQueue is empty')
                raise asyncio.TimeoutError('SQLiteQueue get method timeout')

    def close(self):
        size = self.__len__()
        if size and size > 0:
            self._db.commit()
            self._db.close()
        else:
            self._db.close()
            Path(self._path + '/' + self._name).unlink()

    def qsize(self):
        return self.__len__()

    def empty(self):
        return self.qsize() == 0

    def __len__(self):
        x = self._cursor.execute(self._sql_size)
        return x.fetchone()[0]


class PrioritySQLiteQueue(FifoSQLiteQueue):

    def __init__(self, path='./task', basename='task_priority', maxsize=0, loop=None):
        self._loop = loop if loop else asyncio.get_event_loop()
        self._queues = {}
        self._path = path if path else './task'
        self._basename = basename if basename else 'task_priority'
        self._maxsize = maxsize
        self._pass_or_resume()

    def _pass_or_resume(self):
        task_dir = Path(self._path)
        if not task_dir.is_dir():
            if task_dir.exists():
                task_dir.unlink()
            task_dir.mkdir()
        else:
            activate = task_dir / 'active.json'
            if activate.is_file():
                logging.debug('resume from disk queue')
                with activate.open('r') as fp:
                    x = json.load(fp)
                for priority, filename in x.items():
                    queue = FifoSQLiteQueue(
                            self._path,
                            name=filename,
                            loop=self._loop)
                    self._queues[int(priority)] = queue
            else:
                for i in task_dir.glob('*.db'):
                    i.unlink()

    def _create_queue(self, priority, loop=None):
        name = '{}_{}.db'.format(self._basename, priority)
        queue = FifoSQLiteQueue(self._path, name=name, loop=loop)
        self._queues[priority] = queue

        return queue

    def put_nowait(self, items):
        item, priority = items
        if self.full():
            raise SQLiteFullError('SQLiteQueue is full')

        if priority not in self._queues.keys():
            self._create_queue(priority, loop=self._loop)
        queue = self._queues.get(priority)
        if isinstance(queue, FifoSQLiteQueue):
            queue.put_nowait(item)
        else:
            raise SQLiteError('SQLiteQueue error')

    def get_nowait(self):
        if self.empty():
            raise SQLiteEmptyError('SQLiteQueue is empty')

        for i in sorted(self._queues.keys()):
            queue = self._queues.get(i)
            if queue:
                try:
                    return queue.get_nowait(), i
                except SQLiteEmptyError:
                    continue
        raise SQLiteEmptyError('SQLiteQueue is empty')

    async def put(self, items, block=True, timeout=None):
        if not block:
            self.put_nowait(items)
            return

        if timeout and timeout < 0.0:
            raise ValueError("'timeout' must be a non-negative number")

        start = time.time()
        while True:
            try:
                self.put_nowait(items)
            except SQLiteFullError:
                await asyncio.sleep(0.1, loop=self._loop)
            else:
                break
            if timeout and time.time() - start > timeout:
                if self.full():
                    raise SQLiteFullError('SQLiteQueue is full')
                raise asyncio.TimeoutError('SQLiteQueue put method timeout')

    @property
    def _size(self):
        sizes = [i.qsize() for i in self._queues.values()]
        return sum(sizes)

    def qsize(self):
        return self._size

    def __len__(self):
        return self._size

    def close(self):
        f = Path(self._path) / 'active.json'
        if self._queues:
            x = {}

            for i in self._queues.keys():
                if not self._queues[i].empty():
                    x[i] = self._queues[i]._name
                self._queues[i].close()
            if x:
                with f.open('w') as fp:
                    json.dump(x, fp)
        else:
            if f.is_file():
                f.unlink()
