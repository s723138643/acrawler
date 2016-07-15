import pickle

from .queuelib import sqlitequeue as sqliteq
from .queuelib.sqlitequeue import SQLiteEmptyError, SQLiteFullError


def _serializeQueue(queueClass, pickleFun, unpickleFun):
    class SerialzeQueue(queueClass):

        def put_nowait(self, item):
            n = pickleFun(item)
            queueClass.put_nowait(self, n)

        def get_nowait(self):
            item = queueClass.get_nowait(self)
            return unpickleFun(item)

    return SerialzeQueue


def _serializePriorityQueue(queueClass, pickleFun, unpickleFun):
    class SerialzeQueue(queueClass):

        def put_nowait(self, items):
            item, priority = items
            n = pickleFun(item)
            super(SerialzeQueue, self).put_nowait((n, priority))

        def get_nowait(self):
            item, priority = super(SerialzeQueue, self).get_nowait()
            return unpickleFun(item), priority

    return SerialzeQueue


def pickleNode(node):
    return pickle.dumps(node)


def unpickleNode(node):
    return pickle.loads(node)


FifoSQLiteQueue = _serializeQueue(
        sqliteq.FifoSQLiteQueue,
        pickleNode,
        unpickleNode)

PrioritySQLiteQueue = _serializePriorityQueue(
        sqliteq.PrioritySQLiteQueue,
        pickleNode,
        unpickleNode)
