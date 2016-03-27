import os
import sqlite3
import pickle
import time

import asyncio

#from .basequeue import Empty, Full

class SQLiteEmptyError(Exception):
    pass

class SQLiteFullError(Exception):
    pass

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

    def __init__(self, path, maxsize=0, loop=None):
        self._maxsize = maxsize if maxsize else 0
        self._loop = loop if loop else asyncio.get_event_loop()

        self._path = os.path.abspath(path)
        self._db = sqlite3.Connection(self._path, timeout=60)
        self._db.text_factory = bytes
        self._cursor = self._db.cursor()
        self._cursor.execute(self._sql_create)

    def put_nowait(self, item):
        if self.full():
            raise SQLiteFullError('SQLiteQueue is Full')
        x = pickleit(item)
        self._cursor.execute(self._sql_push, (x,))

    def get_nowait(self):
        if self.empty():
            raise SQLiteEmptyError('SQLiteQueue is empty')

        x = self._cursor.execute(self._sql_pop)
        try:
            _id, _item = x.fetchone()
            self._cursor.execute(self._sql_del, (_id,))
        except TypeError as e:
            raise e
        return unpickleit(_item)

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
            os.remove(self._path)

    def __del__(self):
        self.close()

    def qsize(self):
        return self.__len__()

    def empty(self):
        return self.qsize() == 0

    def __len__(self):
        x = self._cursor.execute(self._sql_size)
        return x.fetchone()[0]


class PrioritySQLiteQueue(FifoSQLiteQueue):
    _sql_create = (
            'CREATE TABLE IF NOT EXISTS priorityqueue '
            '(id INTEGER PRIMARY KEY AUTOINCREMENT, priority \
                    INTEGER, item BLOB, del INTEGER)'
            )
    _sql_push = 'INSERT INTO priorityqueue (priority,item,del) VALUES (?,?,?)'
    _sql_pop = 'SELECT id, item FROM priorityqueue \
            WHERE del=0 ORDER BY priority, id LIMIT 1'
    _sql_size = 'SELECT COUNT(*) FROM priorityqueue WHERE del=0'
    _sql_del_immedialte = 'DELETE FROM priorityqueue WHERE id = ?'
    _sql_del_late = 'UPDATE priorityqueue SET del=1 WHERE id = ?'
    _sql_del = 'DELETE FROM priorityqueue WHERE del=1'

    def __init__(self, path, maxsize=0, loop=None):
        self._path = os.path.abspath(path)
        self._db = sqlite3.Connection(self._path)
        self._db.text_factory = bytes

        self._maxsize = maxsize if maxsize else 0
        self._loop = loop if loop else asyncio.get_event_loop()

        self._cursor = self._db.cursor()
        self._cursor.execute(self._sql_create)

    def put_nowait(self, item):
        if self.full():
            raise SQLiteFullError('SQLiteQueue is Full')
        x = pickleit(item)
        self._cursor.execute(self._sql_push, (item.priority, x, 0))

    def get_nowait(self):
        if self.empty():
            raise SQLiteEmptyError('SQLiteQueue is empty')

        x = self._cursor.execute(self._sql_pop)
        try:
            _id, _item = x.fetchone()
            self._cursor.execute(self._sql_del_late, (_id,))
        except TypeError as e:
            raise e
        return unpickleit(_item)

    def close(self):
        size = self.__len__()
        if size and size > 0:
            self._cursor.execute(self._sql_del)
            self._db.commit()
            self._db.close()
        else:
            os.remove(self._path)
