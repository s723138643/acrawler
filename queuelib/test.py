import asyncio
import time

import sqlitequeue

async def testqueue():
    datas = [1, 2, 3, 4, 5, 6, 7]
    queue = sqlitequeue.FifoSQLiteQueue('test.db')
    for i in datas:
        print('put data: {}'.format(i))
        await queue.put(i)
    print('-' * 10)
    for i in range(4):
        x = await queue.get(timeout=1)
        print('get data: {}'.format(x))

class item:
    def __init__(self, data, priority):
        self.data = data
        self.priority = priority

async def testpriorityqueue():
    datas = [
            item(1,2), item(2,2), item(3,2),
            item(4,1), item(5,3), item(6,2),
            item(7,1), item(8,3), item(9,2)
            ]
    queue = sqlitequeue.PrioritySQLiteQueue('test.db', 10)
    for i in range(5):
        print('put data: {}, priority: {}'.format(datas[i].data, datas[i].priority))
        await queue.put(datas[i])

    print('queue size {}'.format(queue.qsize()))
    print('-' * 10)
    for i in range(3):
        x = await queue.get()
        print('get data: {}, priority: {}'.format(x.data, x.priority))

    print('queue size {}'.format(queue.qsize()))
    for i in range(1, 5):
        start = time.time()
        try:
            x = await queue.get(timeout=i)
            print('get data: {}, priority: {}'.format(x.data, x.priority))
        except Exception:
            print('wait {} seconds, actually {} seconds'.format(
                i, time.time()-start))

    for i in range(15):
        print('put data:{}, priority: {}'.format(
            datas[i%len(datas)].
            data,datas[i%len(datas)].priority))
        start = time.time()
        try:
            x = await queue.put(datas[i%len(datas)], timeout=2)
        except sqlitequeue.SQLiteFullError:
            print('wait 2 seconds, actually {} seconds'.format(
                time.time()-start))


async def work():
    count = 0
    while count < 10:
        count += 1
        print('got work {}'.format(count))
        await asyncio.sleep(1)

task = [
#        asyncio.ensure_future(work()),
        asyncio.ensure_future(testpriorityqueue())
        ]

loop = asyncio.get_event_loop()
#loop.run_until_complete(testqueue())
print('*'*10)
loop.run_until_complete(asyncio.wait(task))
loop.close()
