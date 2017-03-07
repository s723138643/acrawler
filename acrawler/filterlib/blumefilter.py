import logging
import pathlib

import pyblume

from .basefilter import BaseFilter

logger = logging.getLogger('Scheduler.Filter')


class BlumeFilter(BaseFilter):
    def __init__(self, settings):
        super().__init__(settings)
        self._blumeclosed = False
        path = settings.get('db_path')
        self._path = pathlib.Path(path)
        if not self._path.exists():
            self._path.mkdir()
        blumefile = settings.get('blumedb')
        self._blumedb = self._path / blumefile
        if self._blumedb.is_file():
            self._blumefilter = pyblume.open(str(self._blumedb), for_write=1)
        else:
            self._blumefilter = pyblume.Filter(
                    1024*1024*10, 0.000001, str(self._blumedb))

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
        super().close()
        self._blumefilter.close()
        self._blumeclosed = True

    def __del__(self):
        if not self._blumeclosed:
            logger.warn('Filter not closed')
            self.close()
