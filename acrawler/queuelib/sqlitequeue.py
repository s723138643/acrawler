import re
import sqlite3
import asyncio
import logging

from pathlib import Path

from .base import Empty, Full, BaseQueue, serialze, unserialze


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
_sql_pop = 'SELECT id,serialzed FROM queue ORDER BY id LIMIT 1'
_sql_del = 'DELETE FROM queue WHERE id = ?'


class FifoSQLiteQueue(BaseQueue):

    def __init__(self, settings, maxsize=0, loop=None):
        super().__init__(loop=loop)
        self._closed = False
        self._maxsize = maxsize if maxsize else 0
        self._loop = loop if loop else asyncio.get_event_loop()
        self._settings = settings
        self._path = settings.get('sqlite_path', './')
        self._name = settings.get('sqlite_dbname', 'task.db')
        self._dbfile = Path(self._path) / self._name

        self._db = sqlite3.Connection(str(self._dbfile), timeout=60)
        self._db.row_factory = sqlite3.Row
        cursor = self._db.cursor()
        try:
            cursor.execute(_sql_create)
        finally:
            cursor.close()

    def _put(self, request):
        try:
            serialzed = serialze(request)
        except Exception as e:
            logger.error('Serialze Error, {}'.format(e))
            return
        cursor = self._db.cursor()
        try:
            cursor.execute(_sql_push, (request.url, request.priority,
                                       request.created, request.last_activated,
                                       request.retryed, serialzed))
        finally:
            cursor.close()

    def _get(self):
        while True:
            cursor = self._db.cursor()
            try:
                row = cursor.execute(_sql_pop).fetchone()
                if not row:
                    raise Empty
                cursor.execute(_sql_del, (row['id'],))
                try:
                    unserialzed = unserialze(row['serialzed'])
                except Exception as e:
                    logger.error('Unserialzed Error, {}'.format(e))
                else:
                    return unserialzed
            finally:
                cursor.close()

    @property
    def maxsize(self):
        return self._maxsize

    @staticmethod
    def clean(settings):
        dbpath = Path(settings.get('sqlite_path', './'))
        dbfile = dbpath / settings.get('sqlite_dbname', 'task.db')
        if dbfile.is_file():
            dbfile.unlink()

    def close(self):
        if not self._closed:
            self._closed = True
            if self.__len__() > 0:
                self._db.commit()
                self._db.close()
            else:
                self._db.close()
                self._dbfile.unlink()

    def is_closed(self):
        return self._closed

    def qsize(self):
        return self.__len__()

    def __len__(self):
        cursor = self._db.cursor()
        try:
            x = cursor.execute(_sql_size)
            return x.fetchone()[0]
        finally:
            cursor.close()

    def __del__(self):
        if not self._closed:
            self.close()


class PrioritySQLiteQueue(BaseQueue):

    def __init__(self, settings, loop=None):
        super(PrioritySQLiteQueue, self).__init__(loop=loop)
        self._closed = False
        self._loop = loop if loop else asyncio.get_event_loop()
        self._queues = {}
        self._path = settings.get('sqlite_path', './')
        self._basename = settings.get('sqlite_dbname', 'task_priority')
        self._maxsize = settings.get('maxsize', 0)

        self._pass_or_resume()

    def _pass_or_resume(self):
        task_dir = Path(self._path)
        if not task_dir.is_dir():
            task_dir.mkdir()
            return

        for child in task_dir.iterdir():
            if child.is_file():
                m = re.match(self._basename+'_(?P<p>\d+)\.db', str(child))
                if not m:
                    continue
                priority = int(m.group('p'))
                tmp = {
                    'sqlite_path': self._path,
                    'sqlite_dbname': child.name
                }
                queue = FifoSQLiteQueue(tmp, loop=self._loop)
                self._queues[priority] = queue

    def _create_queue(self, priority, loop=None):
        name = '{}_{}.db'.format(self._basename, priority)
        tmp = {'sqlite_path': self._path, 'sqlite_dbname': name}
        queue = FifoSQLiteQueue(tmp, loop=loop)
        self._queues[priority] = queue

        return queue

    def _put(self, item):
        priority = item.priority
        if priority not in self._queues.keys():
            queue = self._create_queue(priority, loop=self._loop)
        else:
            queue = self._queues.get(priority)
        queue.put_nowait(item)

    def _get(self):
        for priority, queue in sorted(self._queues.items()):
            try:
                return queue.get_nowait()
            except Empty:
                queue.close()
                del self._queues[priority]
                continue
        raise Empty

    @property
    def _size(self):
        sizes = [i.qsize() for i in self._queues.values()]
        return sum(sizes)

    def qsize(self):
        return self._size

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
