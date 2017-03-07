import logging

from .blumefilter import BlumeFilter
from .dbfilter import SQLiteFilter

logger = logging.getLogger('Scheduler.Filter')


class MixedFilter(BlumeFilter, SQLiteFilter):
    def __init__(self, settings):
        super().__init__(settings)
        self._mixedclosed = False

    def url_seen(self, url):
        unique_url = self.url_normalization(url)
        fingerprint = self.url_fingerprint(unique_url)
        if unique_url in self._blumefilter:
            if self.in_db(fingerprint):
                logger.debug('duplicate request<{}> recived'.format(url))
                return True
            self.db_add(fingerprint)
            return False
        self._blumefilter.add(unique_url)
        self.db_add(fingerprint)
        return False

    @staticmethod
    def clean(settings):
        BlumeFilter.clean(settings)
        SQLiteFilter.clean(settings)

    def close(self):
        self._mixedclosed = True
        super().close()

    def __del__(self):
        if not self._mixedclosed:
            logger.warn('Filter not closed')
            self.close()
