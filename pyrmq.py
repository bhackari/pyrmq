from typing import List
import time
import threading
import redis.asyncio as redis
import asyncio
import uuid


class Delivery(object):

    def __init__(self,
                 connection: redis.Redis,
                 unacked_key: str,
                 rejected_key: str,
                 payload: str):
        self.connection = connection
        self.payload = payload

        self.unacked_key = unacked_key
        self.rejected_key = rejected_key

    async def ack(self):
        return await self.connection.lrem(self.unacked_key, 1, self.payload)

    async def reject(self):
        return await self.connection.lpush(self.rejected_key, self.payload)


class Deliveries(object):

    def __init__(self, deliveries: List[Delivery]):
        self.deliveries = deliveries

    async def ack(self):
        return await asyncio.gather(*[delivery.ack() for delivery in self.deliveries])

    async def reject(self):
        return await asyncio.gather(*[delivery.reject() for delivery in self.deliveries])


# TODO(mirco): implement error handling in case of timeouts
class Queue(object):

    def __init__(self, connection: redis.Redis, queue_name: str):
        self.connection = connection
        self.queue_name = queue_name

        connection_uuid = uuid.uuid4()
        self.ready_key = f'rmq::queue::[{queue_name}]::ready'
        self.rejected_key = f'rmq::queue::[{queue_name}]::rejected'
        self.unacked_key = f'rmq::connection::[{connection_uuid}]::queue::[{queue_name}]::unacked'

        self.tasks = []

    async def wait(self):
        return await asyncio.wait(self.tasks)

    async def publish(self, payload: str):
        await self.connection.lpush(self.ready_key, payload)

    def attach_consumer(self, consumer):
        async def wrapper():
            while True:
                payload = await self.connection.rpoplpush(self.ready_key, self.unacked_key)
                delivery = Delivery(self.connection, self.unacked_key, self.rejected_key, payload)
                await consumer(delivery)

        self.tasks.append(asyncio.create_task(wrapper()))

    def attach_batch_consumer(self, size: int, timeout: int, consumer):
        async def wrapper():
            deliveries = []

            async def consume(deliveries):
                while True:
                    payload = await self.connection.rpoplpush(self.ready_key, self.unacked_key)

                    delivery = Delivery(self.connection, self.unacked_key, self.rejected_key, payload)
                    deliveries.append(delivery)

                    if len(deliveries) >= size:
                        return

            while True:
                try:
                    await asyncio.wait_for(consume(deliveries), timeout=timeout)
                except asyncio.exceptions.TimeoutError:
                    await consumer(Deliveries(deliveries))
                    deliveries = []

                if len(deliveries) >= size:
                    await consumer(Deliveries(deliveries))
                    deliveries = []

        self.tasks.append(asyncio.create_task(wrapper()))
