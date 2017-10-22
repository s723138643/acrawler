import re
import sqlite3
import asyncio
import logging
from collections import defaultdict
from pathlib import Path

from .base import QueueEmpty, BaseQueue, serialze, unserialze


logger = logging.getLogger(name='Scheduler.Queue')


_sql_create = ('CREATE TABLE IF NOT EXISTS queue '
               '(id INTEGER PRIMARY KEY AUTOINCREMENT,'
               ' url TEXT,'
               ' priority INTEGER,'
               ' created REAL,'
               ' last_activated REAL,'
               ' retryed INTEGER,'
               ' serialzed BLOB)')
_sql_size = 'SELECT COUNT(*) FROM queue'
_sql_push = ('INSERT INTO queue '
             '(url, priority, created,'
             ' last_activated, retryed, serialzed) '
             'VALUES (?,?,?,?,?,?)')
_sql_pop = 'SELECT id,serialzed FROM queue ORDER BY id LIMIT ?'
_sql_del = 'DELETE FROM queue WHERE id = ?'


class SQLite:
    def _execute(self, db, query, data=None):
        cursor = db.cursor()
        try:
            if data:
                cursor.execute(query, data)
            else:
                cursor.execute(query)
        finally:
            cursor.close()

    def _execute_many(self, db, query, data=None):
        cursor = db.cursor()
        try:
            cursor.executemany(query, data)
            return cursor.rowcount
        finally:
            cursor.close()

    def _fetch_one(self, db, query, data=None):
        cursor = db.cursor()
        try:
            if data:
                cursor.execute(query, data)
            else:
                cursor.execute(query)
            return cursor.fetchone()[0]
        finally:
            cursor.close()

    def _fetch_all(self, db, query, data=None):
        cursor = db.cursor()
        try:
            if data:
                cursor.execute(query, data)
            else:
                cursor.execute(query)
            for record in cursor:
                yield record
        finally:
            cursor.close()


class FifoSQLiteQueue(SQLite, BaseQueue):

    def __init__(self, settings, loop=None):
        BaseQueue.__init__(self, loop=loop)
        self._closed = False
        self._loop = loop if loop else asyncio.get_event_loop()
        self._settings = settings
        self._path = settings.get('sqlite_path', './')
        self._name = settings.get('sqlite_dbname', 'task.db')
        self._dbfile = Path(self._path) / self._name

        self._db = sqlite3.Connection(str(self._dbfile), timeout=60)
        self._db.row_factory = sqlite3.Row
        self._create_table()

    def _create_table(self):
        self._execute(self._db, _sql_create)

    def _put(self, items):
        records = []
        for item in items:
            try:
                serialzed = serialze(item)
            except Exception as e:
                logger.error('Serialze Error, {}'.format(e))
                continue
            record = (
                item.url, item.priority, item.created,
                item.last_activated, item.retryed, serialzed
                )
            records.append(record)
        return self._execute_many(self._db, _sql_push, records)

    def _get(self, count=1):
        items, deletes = [], []
        for row in self._fetch_all(self._db, _sql_pop, (count,)):
            try:
                unserialzed = unserialze(row['serialzed'])
            except Exception as e:
                logger.error(e)
                continue
            items.append(unserialzed)
            deletes.append((row['id'], ))
        self._execute_many(self._db, _sql_del, deletes)
        if not items:
            raise QueueEmpty()
        return items

    @staticmethod
    def clean(settings):
        dbpath = Path(settings.get('sqlite_path', './'))
        dbfile = dbpath / settings.get('sqlite_dbname', 'task.db')
        if dbfile.is_file():
            dbfile.unlink()

    def close(self):
        if not self._closed:
            self._closed = True
            if self.qsize() > 0:
                self._db.commit()
                self._db.close()
            else:
                self._db.close()
                self._dbfile.unlink()

    def is_closed(self):
        return self._closed

    def qsize(self):
        return self._fetch_one(self._db, _sql_size)

    def __del__(self):
        if not self._closed:
            self.close()


class PrioritySQLiteQueue(BaseQueue):

    def __init__(self, settings, loop=None):
        BaseQueue.__init__(loop=loop)
        self._closed = False
        self._loop = loop if loop else asyncio.get_event_loop()
        self._queues = {}
        self._path = settings.get('sqlite_path', './')
        self._basename = settings.get('sqlite_dbname', 'task_priority')
        self._pass_or_resume()

    def _got_db(self, path: Path):
        for child in path.iterdir():
            if child.is_file():
                m = re.search(self._basename+'_(?P<p>\d+)\.db', str(child))
                if not m:
                    continue
                priority = int(m.group('p'))
                tmp = {
                    'sqlite_path': str(path),
                    'sqlite_dbname': child.name
                }
                yield priority, FifoSQLiteQueue(tmp, loop=self._loop)

    def _pass_or_resume(self):
        task_dir = Path(self._path)
        if not task_dir.is_dir():
            task_dir.mkdir()
            return
        for priority, db in self._got_db(task_dir):
            self._queues[priority] = db

    def _create_queue(self, priority, loop=None):
        name = '{}_{}.db'.format(self._basename, priority)
        tmp = {'sqlite_path': self._path, 'sqlite_dbname': name}
        queue = FifoSQLiteQueue(tmp, loop=loop)
        self._queues[priority] = queue
        return queue

    def _put(self, items):
        maps = defaultdict([])
        for item in items:
            maps[item.priority].append(item)
        for priority, requests in maps.items():
            if priority not in self._queues:
                queue = self._create_queue(priority, loop=self._loop)
            else:
                queue = self._queues.get(priority)
            queue.put_nowait(requests)

    def _get(self, count=1):
        results = []
        remain = count
        for priority, queue in sorted(self._queues.items()):
            try:
                items = queue.get_nowait(remain)
            except QueueEmpty:
                queue.close()
                del self._queues[priority]
                continue
            results.extend(items)
            remain -= len(items)
            if remain <= 0:
                return results
        if not results:
            raise QueueEmpty()
        return results

    def qsize(self):
        sizes = [i.qsize() for i in self._queues.values()]
        return sum(sizes)

    @staticmethod
    def clean(settings):
        dbpath = Path(settings.get('sqlite_path', './'))
        if not dbpath.is_dir():
            return
        basename = settings.get('sqlite_dbname', 'task_priority')
        for f in dbpath.iterdir():
            if f.match(basename+'_*.db'):
                f.unlink()

    def close(self):
        self._closed = True
        for queue in self._queues.values():
            queue.close()

    def is_closed(self):
        return self._closed
