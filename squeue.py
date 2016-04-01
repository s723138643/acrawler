from .queuelib import sqlitequeue as sqliteq
from .queuelib.sqlitequeue import SQLiteEmptyError, SQLiteFullError

def _serializeQueue(queueClass, pickleFun):
    class SerialzeQueue(queueClass):
        def __init__(self, path='.', name='task.db', maxsize=0, loop=None):
            queueClass.__init__(self, path=path, maxsize=maxsize, loop=loop)

        def put_nowait(self, item):
            n = pickleFun(item)
            queueClass.put_nowait(self, n)

        async def put(self, item, block=True, timeout=None):
            await queueClass.put(self, item)

    return SerialzeQueue

def _serializePriorityQueue(queueClass, pickleFun):
    class SerialzeQueue(queueClass):
        def __init__(self, path='./task', maxsize=0, loop=None):
            queueClass.__init__(self, path=path, maxsize=maxsize, loop=loop)

        def put_nowait(self, item, priority):
            n = pickleFun(item)
            queueClass.put_nowait(self, n, priority)

        async def put(self, item, block=True, timeout=None):
            await queueClass.put(self, item, block, timeout)

    return SerialzeQueue

def pickleNode(node):
    node.callback = node.callback.__name__ if node.callback else None
    node.fetch_fun = node.fetch_fun.__name__ if node.fetch_fun else None
    return node

FifoSQLiteQueue = _serializeQueue(sqliteq.FifoSQLiteQueue, pickleNode)
PrioritySQLiteQueue = _serializePriorityQueue(sqliteq.PrioritySQLiteQueue, pickleNode)
