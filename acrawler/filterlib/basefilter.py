import logging
import urllib.parse as uparse
from hashlib import sha1


logger = logging.getLogger('Scheduler.Filter')


def get_deep(path):
    p = path.strip('/')
    return len(p.split('/'))


class BaseFilter:
    allowed_hosts = set()
    allowed_schemes = ['http', 'https']

    def __init__(self, settings):
        self.settings = settings
        self.hostonly = self.settings.get('hostonly')
        self.maxdeep = self.settings.get('maxdeep')

    @classmethod
    def set_hosts(cls, hosts):
        cls.allowed_hosts.update(hosts)

    def allowed(self, request):
        components = uparse.urlparse(request.url)

        if not self.scheme_ok(components.scheme):
            logger.debug('{} is not a valid URL'.format(request.url))
            return False
        if self.hostonly and not self.host_ok(components.netloc):
            logger.debug('{} not in host lists {}'
                         .format(components.netloc, self.allowed_hosts))
            return False
        if self.maxdeep and self.maxdeep > 0:
            deep = get_deep(components.path)
            if deep > self.maxdeep:
                logger.debug('path:{} is too deep'.format(components.path))
                return False
        if self.url_seen(request.url):
            return False

        return True

    def scheme_ok(self, scheme):
        return scheme in self.allowed_schemes

    def host_ok(self, host):
        return host in self.allowed_hosts

    def url_seen(self, url):
        return False

    def url_fingerprint(self, url):
        durl = url.encode()
        return sha1(durl).hexdigest()

    def url_normalization(self, url):
        components = uparse.urlparse(url)
        ql = uparse.parse_qsl(components.query)
        unique_query = uparse.urlencode(sorted(ql))    # make query unique
        params = (components.scheme,
                  components.netloc,
                  components.path.rstrip('/'),
                  components.params,
                  unique_query,
                  '')
        return uparse.urlunparse(params)

    def close(self):
        pass
