import pickle

from .queuelib import sqlitequeue as sqliteq
from .queuelib.sqlitequeue import SQLiteEmptyError, SQLiteFullError

def _serializeQueue(queueClass, pickleFun, unpickleFun):
    class SerialzeQueue(queueClass):
        def __init__(self, path='.', name='task.db', maxsize=0, loop=None):
            queueClass.__init__(self, path=path, maxsize=maxsize, loop=loop)

        def put_nowait(self, item):
            n = pickleFun(item)
            queueClass.put_nowait(self, n)

        def get_nowait(self):
            item = queueClass.get_nowait(self)
            return unpickleFun(item)

    return SerialzeQueue

def _serializePriorityQueue(queueClass, pickleFun, unpickleFun):
    class SerialzeQueue(queueClass):
        def __init__(self, path='./task', maxsize=0, loop=None):
            super(SerialzeQueue, self).__init__(path=path, maxsize=maxsize, loop=loop)

        def put_nowait(self, items):
            item, priority = items
            n = pickleFun(item)
            super(SerialzeQueue, self).put_nowait((n, priority))

        def get_nowait(self):
            item, priority = super(SerialzeQueue, self).get_nowait()
            return unpickleFun(item), priority

    return SerialzeQueue

def pickleNode(node):
    node.callback = node.callback.__name__ if node.callback else None
    node.fetch_fun = node.fetch_fun.__name__ if node.fetch_fun else None

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
