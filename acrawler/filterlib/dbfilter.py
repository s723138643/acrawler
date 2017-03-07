import logging
import sqlite3
import pathlib

from .basefilter import BaseFilter

logger = logging.getLogger('Scheduler.Filter')

_sql_create = ('CREATE TABLE IF NOT EXISTS urls'
               '(id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,'
               ' fingerprint CHAR(40) NOT NULL UNIQUE)')

_sql_put = 'INSERT INTO urls (fingerprint) VALUES (?)'
_sql_query = 'SELECT fingerprint FROM urls WHERE fingerprint=?'


class SQLiteFilter(BaseFilter):
    def __init__(self, settings):
        super().__init__(settings)
        self._sqliteclosed = False
        path = settings.get('db_path')
        self._path = pathlib.Path(path)
        if not self._path.exists():
            self._path.mkdir()
        sqlite3file = settings.get('sqlitedb')
        self._db = sqlite3.Connection(str(self._path / sqlite3file))
        cursor = self._db.cursor()
        try:
            cursor.execute(_sql_create)
        finally:
            cursor.close()

    def url_seen(self, url):
        unique_url = self.url_normalization(url)
        fingerprint = self.url_fingerprint(unique_url)
        if self.in_db(fingerprint):
            logger.debug('duplicate request<{}> recived'.format(url))
            return True
        self.db_add(fingerprint)
        return False

    def db_add(self, fingerprint):
        cursor = self._db.cursor()
        try:
            cursor.execute(_sql_put, (fingerprint,))
        finally:
            cursor.close()

    def in_db(self, fingerprint):
        # print('SQLite test url {}'.format(url))
        cursor = self._db.cursor()
        try:
            result = cursor.execute(_sql_query, (fingerprint, ))
            return result.fetchone() is not None
        finally:
            cursor.close()

    @staticmethod
    def clean(settings):
        path = pathlib.Path(settings['db_path'])
        sqlitedb = path / settings['sqlitedb']
        if sqlitedb.is_file():
            sqlitedb.unlink()

    def close(self):
        self._sqliteclosed = True
        super().close()
        self._db.commit()
        self._db.close()

    def __del__(self):
        if not self._sqliteclosed:
            logger.warn('Filter not closed')
            self.close()
