import logging

from .blumefilter import BlumeFilter
from .dbfilter import SQLiteFilter

logger = logging.getLogger('Filter')


class MixedFilter(BlumeFilter, SQLiteFilter):
    def __init__(self, settings):
        super().__init__(settings)
        self._mixedclosed = False

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

    @staticmethod
    def clean(settings):
        BlumeFilter.clean(settings)
        SQLiteFilter.clean(settings)

    def close(self):
        super().close()
        self._mixedclosed = True

    def __del__(self):
        if not self._mixedclosed:
            logger.warn('Filter not closed')
            self.close()
