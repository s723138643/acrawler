from .queuelib import sqlitequeue as sqliteq
from .queuelib.basequeue import Empty
from .queuelib.basequeue import Full

def _serializeQueue(queueClass, pickleFun):
    class SerialzeQueue(queueClass):
        def __init__(self, path, maxsize=None):
            queueClass.__init__(self, path, maxsize)

        def put_nowait(self, item):
            n = pickleFun(item)
            queueClass.put_nowait(self, n)

        async def put(self, item):
            await queueClass.put(self, item)

    return SerialzeQueue

def pickleNode(node):
    node.callback = node.callback.__name__ if node.callback else None
    return node

FifoSQLiteQueue = _serializeQueue(sqliteq.FifoSQLiteQueue, pickleNode)
PrioritySQLiteQueue = _serializeQueue(sqliteq.PrioritySQLiteQueue, pickleNode)
