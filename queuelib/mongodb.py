import pymongo

from .base import BaseQueue, Empty


class PriorityMongoQueue(BaseQueue):

    def __init__(self, settings, maxsize=0, loop=None):
        super(PriorityMongoQueue, self).__init__(loop=loop)
        self._settings = settings
        self._maxsize = maxsize
        config = settings['mongo_config']
        self._client = pymongo.MongoClient(**config)
        self._db = self._client.get_database(settings['mongo_dbname'])
        self._priorities = set()
        self._basename = settings['mongo_collectionname']
        if self._db.collection_names():
            self._resume()

    def _resume(self):
        for name in self._db.collection_names():
            tmp = name.split('_')
            priority = int(tmp[-1])
            self._priorities.add(priority)

    def _get_collection(self, priority):
        if priority not in self._priorities:
            self._priorities.add(priority)
        return '{}_{}'.format(self._basename, priority)

    def _put(self, items):
        request, priority = items
        col = self._db.get_collection(self._get_collection(priority))
        col.insert_one(request.to_dict())

    def _get(self):
        for priority in sorted(self._priorities):
            col = self._db.get_collection(self._get_collection(priority))
            result = col.find_one_and_delete({})
            if result:
                del result['_id']
                return result, priority
        raise Empty

    @staticmethod
    def clean(settings):
        config = settings['mongo_config']
        client = pymongo.MongoClient(**config)
        try:
            db = client.get_database(settings['mongo_dbname'])
            for collection in db.collection_names():
                db.drop_collection(collection)
        finally:
            client.close()

    def close(self):
        if self._client is not None:
            self._client.close()
            self._client = None

    def __del__(self):
        if self._client is not None:
            self.close()

    def qsize(self):
        size = 0
        for name in self._db.collection_names():
            col = self._db.get_collection(name)
            size += col.find({}).count()
        return size
