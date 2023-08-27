import redis.asyncio as redis
import asyncio

from pyrmq import Queue, Deliveries


async def batch_consumer(delivery: Deliveries):
    print(len(delivery.deliveries))
    return await delivery.ack()


async def main():
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    queue_name = 'foobar'
    queue = Queue(r, queue_name)
    queue.attach_batch_consumer(50000, 3, batch_consumer)
    await queue.wait()

asyncio.run(main())
