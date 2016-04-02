import logging
import urllib.parse as uparse

def get_deep(path):
    p = path.strip('/')
    return len(p.split('/'))

class BaseFilter:

    hosts = set()

    def __init__(self, settings):
        self.settings = settings
        self.hostonly = self.settings.get('hostonly')
        self.maxdeep = self.settings.get('maxdeep')
        self.maxredirect = self.settings.get('maxredirect')

    @classmethod
    def add_hosts(cls, hosts):
        cls.hosts.update(hosts)
        return cls

    @classmethod
    def from_settings(cls, settings):
        return cls(settings)

    def url_allowed(self, url, redirect):
        if not self.url_ok(url):
            return False

        _, host, path, *x = uparse.urlparse(url)

        if self.hostonly and not self.host_ok(host):
            logging.debug('Host Error <{}>'.format(host))
            return False
        if self.maxredirect and 0 < self.maxredirect < redirect:
            logging.debug('Maxredirect Error <{} [{}]>'.format(url, redirect))
            return False
        if self.maxdeep and self.maxdeep > 0:
            deep = get_deep(path)
            if deep > self.maxdeep:
                logging.debug('Maxdeep Error <{} [{}]>'.format(url, maxdeep))
                return False

        url = host + path + ''.join(x)
        return not self.had_seen(url)

    def url_ok(self, url):
        return url.startswith('https://') or url.startswith('http://')

    def host_ok(self, host):
        return host in self.hosts

    def had_seen(self, url):
        return False

    def close(self):
        pass
