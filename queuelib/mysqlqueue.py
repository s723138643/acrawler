import logging

from .base import BaseQueue
from .mysqldb import Connection


class PriorityMysqlQueue(BaseQueue):
    _create = ('CREATE TABLE IF NOT EXISTS {} '
               '(id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,'
               ' url VARCHAR(256) NOT NULL,'
               ' priority INT,'
               ' _fetcher VARCHAR(256),'
               ' _parser VARCHAR(256),'
               ' filter_ignore TINYINT(1),'
               ' redirect INTEGER,'
               ' created FLOAT,'
               ' last_activated FLOAT,'
               ' retryed INTEGER)')
    _insert = ('INSERT INTO {} '
               '(url,priority,_fetcher,_parser,filter_ignore,'
               ' redirect,created,last_activated,retryed) '
               'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)')
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
            logging.debug('create table error, {}'.format(e))
            raise
        finally:
            cur.close()
        self._priorities.add(priority)

    def _put(self, items):
        request, priority = items
        if priority not in self._priorities:
            self._create_table(priority)
        query = self._insert.format(self._make_table_name(priority))
        self._db.execute(query, (request.url, request.priority,
                                 request.fetcher, request.parser,
                                 1 if request.filter_ignore else 0,
                                 request.redirect, request.created,
                                 request.last_activated,
                                 request.retryed))

    def _get(self):
        l = sorted(self._priorities)
        for priority in l:
            query = self._pop.format(self._make_table_name(priority))
            item = self._db.get(query)
            if item:
                query = self._del.format(self._make_table_name(priority))
                self._db.execute(query, (item.id,))
                del item['id']
                return (item, priority)

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
