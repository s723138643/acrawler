class UrlFilter:
    def __init__(self):
        pass

    def addToDB(self, url):
        raise NotImplementedError()

    def inDB(self, url):
        raise NotImplementedError()

    def rmFromDB(self, url):
        raise NotImplementedError()

