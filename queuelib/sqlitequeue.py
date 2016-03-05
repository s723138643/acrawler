import os
import sqlite3
import pickle
import time

import asyncio

from .basequeue import Empty, Full


def pickleit(item):
    return pickle.dumps(item)


def unpickleit(item):
    return pickle.loads(item)


class FifoSQLiteQueue:

    _sql_create = (
        'CREATE TABLE IF NOT EXISTS queue '
        '(id INTEGER PRIMARY KEY AUTOINCREMENT, item BLOB)'
    )
    _sql_size = 'SELECT COUNT(*) FROM queue'
    _sql_push = 'INSERT INTO queue (item) VALUES (?)'
    _sql_pop = 'SELECT id, item FROM queue ORDER BY id LIMIT 1'
    _sql_del = 'DELETE FROM queue WHERE id = ?'

    def __init__(self, path, maxsize=None):
        self._path = os.path.abspath(path)
        self._db = sqlite3.Connection(self._path, timeout=60)
        self._db.text_factory = bytes
        self.maxsize = maxsize
        self.cursor = self._db.cursor()
        self.cursor.execute(self._sql_create)

    def put_nowait(self, item):
        if self.maxsize and self.qsize() >= self.maxsize:
            raise Full('SQLiteQueue is Full')
        x = pickleit(item)
        self.cursor.execute(self._sql_push, (x,))

    def get_nowait(self):
        x = self.cursor.execute(self._sql_pop)
        try:
            _id, _item = x.fetchone()
            self.cursor.execute(self._sql_del, (_id,))
        except TypeError as e:
            if self.empty():
                raise Empty('SQLiteQueue is empty')
            else:
                raise e
        return unpickleit(_item)

    async def put(self, item, block=True, timeout=None):
        if not block:
            self.put_nowait(item)
            return
        start = time.time()
        while True:
            try:
                self.put_nowait(item)
            except Full:
                await asyncio.sleep(0.1)
            else:
                break
            if timeout and time.time() - start > timeout:
                raise asyncio.TimeoutError('SQLiteQueue put method timeout')

    async def get(self, block=True, timeout=None):
        if not block:
            return self.get_nowait()
        start = time.time()
        while True:
            try:
                return self.get_nowait()
            except Empty:
                await asyncio.sleep(0.1)
            if timeout and time.time() - start > timeout:
                raise asyncio.TimeoutError('SQLiteQueue get method timeout')

    def close(self):
        size = self.__len__
        self._db.close()
        if not size:
            os.remove(self._path)

    def qsize(self):
        return self.__len__

    def empty(self):
        return self.__len__ == 0

    @property
    def __len__(self):
        x = self.cursor.execute(self._sql_size)
        return x.fetchone()[0]


class PrioritySQLiteQueue(FifoSQLiteQueue):
    _sql_create = ('CREATE TABLE IF NOT EXISTS queue '
                  '(id INTEGER PRIMARY KEY AUTOINCREMENT, \
                   priority INTEGER, item BLOB, del INTEGER)')
    _sql_push = 'INSERT INTO queue (priority,item,del) VALUES (?,?,0)'
    _sql_pop = 'SELECT id, item FROM queue \
    WHERE del=0 ORDER BY priority, id LIMIT 1'
    _sql_size = 'SELECT COUNT(*) FROM queue WHERE del=0'
    _sql_del_immedialte = 'DELETE FROM queue WHERE id = ?'
    _sql_del_late = 'UPDATE queue SET del=1 WHERE id = ?'
    _sql_del = 'DELETE FROM queue WHERE del=1'

    def __init__(self, path, maxsize=None):
        self._path = os.path.abspath(path)
        self._db = sqlite3.Connection(self._path)
        self._db.text_factory = bytes
        self.maxsize = maxsize
        self.cursor = self._db.cursor()
        self.cursor.execute(self._sql_create)
        self.cursor.execute(self._sql_del)

    def put_nowait(self, item):
        if self.maxsize and self.qsize >= self.maxsize:
            raise Full('SQLiteQueue is Full')
        x = pickleit(item)
        self.cursor.execute(self._sql_push, (item.priority, x))

    def get_nowait(self):
        x = self.cursor.execute(self._sql_pop)
        try:
            _id, _item = x.fetchone()
            self.cursor.execute(self._sql_del_late, (_id,))
        except TypeError as e:
            if self.empty():
                raise Empty('SQLiteQueue is empty')
            else:
                raise e
        return unpickleit(_item)
