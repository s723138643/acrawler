from .model import Request
from .queuelib import sqlitequeue as sqliteq
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
            item, priority = items
            super(SerialzeQueue, self).put_nowait((item, priority))

        def get_nowait(self):
            item, priority = super(SerialzeQueue, self).get_nowait()
            return unpickleFun(item), priority

    return SerialzeQueue


def unpickleNode(node):
    node['filter_ignore'] = True if node['filter_ignore'] > 0 else False
    return Request.from_dict(node)


FifoSQLiteQueue = _serializeQueue(
        sqliteq.FifoSQLiteQueue,
        unpickleNode)

PrioritySQLiteQueue = _serializePriorityQueue(
        sqliteq.PrioritySQLiteQueue,
        unpickleNode)
