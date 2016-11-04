import logging
import sqlite3
import pathlib
from hashlib import sha256

from .basefilter import BaseFilter

logger = logging.getLogger('Filter')

_sql_create = '''CREATE TABLE IF NOT EXISTS urls
 (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
 url VCHAR(255) NOT NULL UNIQUE)'''

_sql_put = 'INSERT INTO urls (url) VALUES (sha256(?))'


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
        self._db.create_function(
                'sha256', 1,
                lambda x: sha256(x.encode()).hexdigest())
        cursor = self._db.cursor()
        try:
            cursor.execute(_sql_create)
        finally:
            cursor.close()

    @staticmethod
    def hash_code(url):
        return sha256(url.encode()).hexdigest()

    def had_seen(self, url):
        if self.is_inSQLite(url):
            logger.debug('url <{}> is crawed, ignore'.format(url))
            return True
        else:
            self.add2SQLite(url)
            return False

    def add2SQLite(self, url):
        self.is_inSQLite(url)

    def is_inSQLite(self, url):
        # print('SQLite test url {}'.format(url))
        cursor = self._db.cursor()
        try:
            cursor.execute(_sql_put, (url, ))
            return False
        except sqlite3.IntegrityError:
            return True
        finally:
            cursor.close()

    @staticmethod
    def clean(settings):
        path = pathlib.Path(settings['db_path'])
        sqlitedb = path / settings['sqlitedb']
        if sqlitedb.is_file():
            sqlitedb.unlink()

    def close(self):
        super().close()
        self._db.commit()
        self._db.close()
        self._sqliteclosed = True

    def __del__(self):
        if not self._sqliteclosed:
            logger.warn('Filter not closed')
            self.close()
