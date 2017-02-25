import re
import logging
import pymysql

from datetime import datetime
from .base import BaseQueue, Empty, serialze, unserialze


logger = logging.getLogger('Scheduler.Queue')


def to_timestamp(time):
    return datetime.fromtimestamp(time).strftime('%Y-%m-%d %H:%M:%S')


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
        self._closed = False
        super(PriorityMysqlQueue, self).__init__(loop=loop)
        assert type(maxsize) is int, 'maxsize not an integer'
        self.maxsize = maxsize
        self._settings = settings
        self._basename = settings['mysql_tablename']
        config = settings['mysql_config']
        self._db = pymysql.connect(**config)
        self._priorities = set()
        self._total = 0
        self.resume()

    def resume(self):
        cur = self._db.cursor()
        total = 0
        try:
            cur.execute('SHOW TABLES;')
            tables = cur.fetchall()
            for t in tables:
                m = re.match(self._basename+'_(?P<p>\d+)', t[0])
                if m:
                    priority = int(m.group('p'))
                    self._priorities.add(priority)
                cur.execute(self._qsize.format(t[0]))
                total += cur.fetchone()[0]
        finally:
            cur.close()
        self._total = total

    def _make_table_name(self, priority):
        return self._basename + '_' + str(priority)

    def _create_table(self, priority):
        cur = self._db.cursor()
        try:
            query = self._create.format(
                        self._make_table_name(priority))
            cur.execute(query)
        finally:
            cur.close()
        self._priorities.add(priority)

    def _put(self, request):
        try:
            serialzed = serialze(request)
        except Exception as e:
            logger.error('Serialze Error, {}'.format(e))
            return
        priority = request.priority
        if priority not in self._priorities:
            self._create_table(priority)
        query = self._insert.format(self._make_table_name(priority))
        cur = self._db.cursor()
        try:
            cur.execute(query, (request.url, priority,
                                to_timestamp(request.created),
                                to_timestamp(request.last_activated),
                                request.retryed, serialzed))
            self._total += 1
        finally:
            cur.close()

    def _get(self):
        for priority in sorted(self._priorities):
            query = self._pop.format(self._make_table_name(priority))
            cur = self._db.cursor()
            try:
                cur.execute(query)
                result = cur.fetchone()
                if not result:
                    continue
            finally:
                cur.close()
            query = self._del.format(self._make_table_name(priority))
            cur = self._db.cursor()
            try:
                cur.execute(query, (result[0],))
                self._total -= 1
            finally:
                cur.close()

            try:
                unserialzed = unserialze(result[6])
            except Exception as e:
                logger.error('Unserialz Error, {}'.format(e))
            else:
                return unserialzed
        raise Empty

    @staticmethod
    def clean(settings):
        config = settings['mysql_config']
        db = pymysql.connect(**config)
        cur = db.cursor()
        try:
            cur.execute('show tables;')
            for table in cur.fetchall():
                cur.execute('drop table ' + table[0])
        finally:
            cur.close()
        db.commit()
        db.close()

    def qsize(self):
        return self._total

    def close(self):
        self._closed = True
        self._db.commit()
        self._db.close()

    def __del__(self):
        if not self._closed:
            self.close()
