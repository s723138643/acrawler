import re
import sqlite3
import asyncio

from pathlib import Path

from .base import Empty, Full, BaseQueue

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


class FifoSQLiteQueue(BaseQueue):

    def __init__(self, settings, maxsize=0, loop=None):
        super(FifoSQLiteQueue, self).__init__(loop=loop)
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
        cursor.execute(_sql_create)

    def _put(self, request):
        cursor = self._db.cursor()
        try:
            cursor.execute(_sql_push,
                           (request.url, request.priority, request.fetcher,
                            request.parser, 1 if request.filter_ignore else 0,
                            request.redirect, request.created,
                            request.last_activated, request.retryed))
        finally:
            cursor.close()

    def _get(self):
        cursor = self._db.cursor()
        try:
            x = cursor.execute(_sql_pop)
            row = x.fetchone()
            cursor.execute(_sql_del, (row['id'],))
            result_dict = dict(row)
            del result_dict['id']
            return result_dict
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
        self._closed = True
        size = self.__len__()
        if size > 0:
            self._db.commit()
            self._db.close()
        else:
            self._db.close()
            self.clean(self._settings)

    def is_closed(self):
        return self._closed

    def qsize(self):
        return self.__len__()

    def __len__(self):
        cursor = self._db.cursor()
        x = cursor.execute(_sql_size)
        return x.fetchone()[0]

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
        else:
            for child in task_dir.iterdir():
                if child.is_file() and child.match(self._basename+'_*.db'):
                    m = re.search(self._basename+'_(.*)\.db', str(child))
                    if m:
                        priority = m.group(1)
                        tmp = {'sqlite_path': self._path,
                               'sqlite_dbname': child.name}
                        queue = FifoSQLiteQueue(tmp, loop=self._loop)
                        self._queues[int(priority)] = queue

    def _create_queue(self, priority, loop=None):
        name = '{}_{}.db'.format(self._basename, priority)
        tmp = {'sqlite_path': self._path, 'sqlite_dbname': name}
        queue = FifoSQLiteQueue(tmp, loop=loop)
        self._queues[priority] = queue

        return queue

    def _put(self, items):
        request, priority = items
        if priority not in self._queues.keys():
            self._create_queue(priority, loop=self._loop)
        queue = self._queues.get(priority)
        if isinstance(queue, FifoSQLiteQueue):
            queue.put_nowait(request)

    def _get(self):
        for priority in sorted(self._queues.keys()):
            queue = self._queues[priority]
            if isinstance(queue, FifoSQLiteQueue):
                try:
                    return (queue.get_nowait(), priority)
                except Empty:
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
        basename = settings.get('sqlite_dbname', 'task_priority')
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
