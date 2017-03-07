import logging
import pathlib

from .basefilter import BaseFilter

logger = logging.getLogger("Scheduler.Filter")


class MemFilter(BaseFilter):
    def __init__(self, settings):
        super().__init__(settings)
        self._closed = False
        path = settings.get('db_path', './')
        self._path = pathlib.Path(path)
        if not self._path.exists():
            self._path.mkdir()

        self.fingerprints = set()
        self._file = self._path / 'filedb'
        self._fd = self._file.open('a+')

        self._initialize()

    def _initialize(self):
        self._fd.seek(0)
        fingerprint = self._fd.readline()
        while fingerprint:
            self.fingerprints.add(fingerprint.strip())
            fingerprint = self._fd.readline()

    def url_seen(self, url):
        unique_url = self.url_normalization(url)
        fingerprint = self.url_fingerprint(unique_url)
        if fingerprint in self.fingerprints:
            logger.debug('duplicate request<{}> recived'.format(url))
            return True

        self.fingerprints.add(fingerprint)
        self._fd.write(fingerprint + '\n')
        return False

    @staticmethod
    def clean(settings):
        path = pathlib.Path(settings.get('db_path', './'))
        fd = path / 'filedb'
        if fd.is_file():
            fd.unlink()

    def close(self):
        if not self._closed:
            self._closed = True
            self.fingerprints.clear()
            self._fd.close()

    def __del__(self):
        if not self._closed:
            logger.warn('Filter not closed')
            self.close()
