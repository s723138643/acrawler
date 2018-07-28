import logging
import pathlib

from pybloom_live import ScalableBloomFilter

from .basefilter import BaseFilter

logger = logging.getLogger('Scheduler.Filter')


class BlumeFilter(BaseFilter):
    def __init__(self, settings):
        self._blumeclosed = False
        super().__init__(settings)
        path = settings.get('db_path')
        self._path = pathlib.Path(path)
        if not self._path.exists():
            self._path.mkdir()
        blumefile = settings.get('blumedb')
        self._blumedb = self._path / blumefile
        if self._blumedb.is_file():
            with self._blumedb.open('rb') as fp:
                self._blumefilter = ScalableBloomFilter.fromfile(fp)
        else:
            growth = ScalableBloomFilter.SMALL_SET_GROWTH
            self._blumefilter = ScalableBloomFilter(10240, 0.001, growth)

    def url_seen(self, url):
        unique_url = self.url_normalization(url)
        if unique_url in self._blumefilter:
            logger.debug('duplicate request<{}> recived'.format(url))
            return True
        self._blumefilter.add(unique_url)
        return False

    @staticmethod
    def clean(settings):
        path = pathlib.Path(settings['db_path'])
        blumedb = path / settings['blumedb']
        if blumedb.is_file():
            blumedb.unlink()

    def close(self):
        self._blumeclosed = True
        super().close()
        with self._blumedb.open('wb') as fp:
            self._blumefilter.tofile(fp)

    def __del__(self):
        if not self._blumeclosed:
            logger.warn('Filter not closed')
            self.close()
