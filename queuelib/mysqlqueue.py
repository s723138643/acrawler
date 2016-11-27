import logging

from .base import BaseQueue
from .mysqldb import Connection


logger = logging.getLogger('Scheduler.Queue')


class PriorityMysqlQueue(BaseQueue):
    _create = ('CREATE TABLE IF NOT EXISTS {} '
               '(id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,'
               ' url VARCHAR(1204) NOT NULL,'
               ' priority INT,'
               ' created TIMESTAMP,'
               ' last_activated TIMESTAMP,'
               ' retryed INTEGER,'
               ' serialzed BLOB NOT NULL)')
    _insert = ('INSERT INTO {} '
               '(url, priority, created, last_activated,'
               ' retryed, serialzed) '
               'VALUES (%s, %s, %s, %s, %s, %s)')
    _pop = 'SELECT * FROM {} ORDER BY id LIMIT 1'
    _del = 'DELETE FROM {} WHERE id=%s'
    _qsize = 'SELECT COUNT(*) FROM {}'

    def __init__(self, settings, maxsize=0, loop=None):
        super(PriorityMysqlQueue, self).__init__(loop=loop)
        assert type(maxsize) is int, 'maxsize not an integer'
        self.maxsize = maxsize
        self._settings = settings
        self._basename = settings['mysql_tablename']
        config = settings['mysql_config']
        self._db = Connection(**config)
        self._priorities = set()

    def _make_table_name(self, priority):
        return '{}_{}'.format(self._basename, priority)

    def _create_table(self, priority):
        cur = self._db.get_cursor()
        try:
            query = self._create.format(
                        self._make_table_name(priority))
            cur.execute(query)
        except Exception as e:
            logger.debug('create table error, {}'.format(e))
            raise
        finally:
            cur.close()
        self._priorities.add(priority)

    def _put(self, request):
        try:
            serialzed = serialze(request)
        except Exception as e:
            logger.error('Serialze Error, {}'.format(e))
            return
        if priority not in self._priorities:
            self._create_table(priority)
        query = self._insert.format(self._make_table_name(priority))
        self._db.execute(query, (request.url, request.priority,
                                 request.created, request.last_activated,
                                 request.retryed, serialzed))

    def _get(self):
        l = sorted(self._priorities)
        for priority in l:
            query = self._pop.format(self._make_table_name(priority))
            item = self._db.get(query)
            if not item:
                continue
            query = self._del.format(self._make_table_name(priority))
            self._db.execute(query, (item.id,))
            try:
                unserialzed = unserialze(item.serialzed)
            except Exception as e:
                logger.error('Unserialz Error, {}'.format(e))
            else:
                return (unserialzed, priority)

    @staticmethod
    def clean(settings):
        config = settings['mysql_config']
        db = Connection(**config)
        tables = db.get_tables()
        if tables:
            for table in tables:
                db.drop_table(table)
        db.close()

    def qsize(self):
        lens = 0
        for priority in self._priorities:
            lens += self._db.len_of_table(self._make_table_name(priority))
        return lens

    def close(self):
        self._db.close()
