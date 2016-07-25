from .model import Request
from .queuelib import sqlitequeue as sqliteq
from .queuelib import mysqlqueue as mysqlq
from .queuelib import mongodb as mongoq
from .queuelib.base import Empty, Full


def _serializeQueue(queueClass, unpickleFun):
    class SerialzeQueue(queueClass):

        def put_nowait(self, request):
            queueClass.put_nowait(self, request)

        def get_nowait(self):
            item = queueClass.get_nowait(self)
            return unpickleFun(item)

    return SerialzeQueue


def _serializePriorityQueue(queueClass, unpickleFun):
    class SerialzeQueue(queueClass):

        def put_nowait(self, items):
            super(SerialzeQueue, self).put_nowait(items)

        def get_nowait(self):
            item, priority = super(SerialzeQueue, self).get_nowait()
            return unpickleFun(item), priority

    return SerialzeQueue


def unpicklefromSQLDB(node):
    node['filter_ignore'] = True if node['filter_ignore'] > 0 else False
    return Request.from_dict(node)


def unpicklefromMongoDB(item):
    return Request.from_dict(item)


FifoSQLiteQueue = _serializeQueue(sqliteq.FifoSQLiteQueue,
                                  unpicklefromSQLDB)

PrioritySQLiteQueue = _serializePriorityQueue(sqliteq.PrioritySQLiteQueue,
                                              unpicklefromSQLDB)

PriorityMysqlQueue = _serializePriorityQueue(mysqlq.PriorityMysqlQueue,
                                             unpicklefromSQLDB)

PriorityMongoQueue = _serializePriorityQueue(mongoq.PriorityMongoQueue,
                                             unpicklefromMongoDB)