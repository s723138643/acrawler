import logging

import pymysql


logger = logging.getLogger('Scheduler.Queue')


class Row(dict):
    def __getattr__(self, row):
        try:
            return self[row]
        except KeyError:
            raise AttributeError(row)


class Connection:
    def __init__(self, *args, **kwargs):
        self._db = None
        self._db_args = args
        self._db_kwargs = kwargs

        try:
            self.reconnect()
        except Exception as e:
            logger.error('Cannot connect to MysqlDB, {}'.format(e))

    def reconnect(self):
        self.close()
        self._db = pymysql.connect(*self._db_args, **self._db_kwargs)
        #self._db.autocommit(True)

    def query(self, query, args=None):
        cursor = self._db.cursor()
        try:
            cursor.execute(query, args)
            column_names = [d[0] for d in cursor.description]
            return [Row(zip(column_names, row)) for row in cursor]
        finally:
            cursor.close()

    def get(self, query, args=None):
        cursor = self._db.cursor()
        try:
            cursor.execute(query, args)
            if cursor.rowcount > 1:
                raise Exception('multiple results getted')
            elif cursor.rowcount == 0:
                return None
            else:
                column_names = [d[0] for d in cursor.description]
                return Row(zip(column_names, cursor.fetchone()))
        finally:
            cursor.close()

    def iter(self, query, args=None):
        cursor = self._db.cursor()
        try:
            cursor.execute(query, args)
            column_names = [d[0] for d in cursor.description]
            for row in cursor:
                yield Row(zip(column_names, row))
        finally:
            cursor.close()

    def len_of_table(self, table):
        cursor = self._db.cursor()
        try:
            cursor.execute('SELECT COUNT(*) FROM {}'.format(table))
            result = cursor.fetchone()
            return result[0]
        finally:
            cursor.close()

    def get_tables(self):
        cursor = self._db.cursor()
        try:
            cursor.execute('SHOW TABLES;')
            return cursor.fetchone()
        finally:
            cursor.close()

    def drop_table(self, table):
        cursor = self._db.cursor()
        try:
            cursor.execute('DROP TABLE IF EXISTS %s', (table,))
        finally:
            cursor.close()

    def get_cursor(self):
        return self._db.cursor()

    def execute(self, query, args=None):
        cursor = self._db.cursor()
        try:
            return cursor.execute(query, args)
        finally:
            cursor.close()

    def executemany(self, query, args=None):
        cursor = self._db.cursor()
        try:
            return cursor.execute(query, args)
        finally:
            cursor.close()

    def close(self):
        if self._db:
            self._db.commit()
            self._db.close()
            self._db = None

    def __del__(self):
        self.close()
