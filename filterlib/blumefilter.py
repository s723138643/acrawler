import logging
import sqlite3
import pathlib
from hashlib import sha256

import pyblume

from .basefilter import BaseFilter

logger = logging.getLogger('Filter')

_sql_create = '''CREATE TABLE IF NOT EXISTS urls
 (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
 url VCHAR(255) NOT NULL UNIQUE)'''

_sql_put = 'INSERT INTO urls (url) VALUES (sha256(?))'


class BlumeFilter(BaseFilter):
    def __init__(self, settings):
        super(BlumeFilter, self).__init__(settings)
        self._closed = False
        path = settings.get('db_path')
        self._path = pathlib.Path(path)
        if not self._path.exists() or not self._path.is_dir():
            self._path.mkdir()
        blumefile = settings.get('blumedb')
        bp = self._path / blumefile
        if bp.is_file():
            self._blumefilter = pyblume.open(str(bp), for_write=1)
        else:
            self._blumefilter = pyblume.Filter(
                    1024*1024*10, 0.000001, str(bp))
        sqlite3file = settings.get('sqlitedb')
        self._db = sqlite3.Connection(str(self._path / sqlite3file))
        self._db.create_function(
                'sha256', 1,
                lambda x: sha256(x.encode()).hexdigest())
        self._cursor = self._db.cursor()
        self._cursor.execute(_sql_create)

    @staticmethod
    def hash_it(url):
        return sha256(url.encode()).hexdigest()

    def had_seen(self, url):
        if url in self._blumefilter:
            if self.is_inSQLite(url):
                logger.debug('url <{}> is crawed, ignore'.format(url))
                return True
            return False
        else:
            self.add2SQLite(url)
            self._blumefilter.add(url)
            return False

    def add2SQLite(self, url):
        self.is_inSQLite(url)

    def is_inSQLite(self, url):
        # print('SQLite test url {}'.format(url))
        try:
            self._cursor.execute(_sql_put, (url, ))
        except sqlite3.IntegrityError:
            return True
        return False

    def close(self):
        self._blumefilter.close()
        self._db.commit()
        self._db.close()
        self._closed = True

    def __del__(self):
        super(BlumeFilter, self).close()
        if not self._closed:
            logger.warn('Filter not closed')
            self.close()
