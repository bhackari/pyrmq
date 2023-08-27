import redis.asyncio as redis
import asyncio
import uuid
import random
import string

from .delivery import Delivery
from .cleaner import Cleaner
from .deliveries import Deliveries


# TODO(mirco): implement error handling in case of timeouts
class Queue(object):

    def __init__(self, connection: redis.Redis, queue_name: str, tag: str):
        self.connection = connection
        self.queue_name = queue_name
        self.tag = tag

        self.connection_uuid = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
        self.connections_key = 'rmq::connections'
        self.ready_key = f'rmq::queue::[{queue_name}]::ready'
        self.rejected_key = f'rmq::queue::[{queue_name}]::rejected'
        self.unacked_key = f'rmq::connection::{tag}-{self.connection_uuid}::queue::[{queue_name}]::unacked'

        # heartbeat
        self.heartbeat_key = f'rmq::connection::{tag}-{self.connection_uuid}::heartbeat'
        self.heartbeat_duration = 60

        self.tasks = []
        self.cleaner = Cleaner(connection, self.connections_key, self.ready_key, self.rejected_key, self.unacked_key)
        self.tasks.append(asyncio.create_task(self.cleaner.clean()))

    async def wait(self):
        return await asyncio.wait(self.tasks)

    async def publish(self, payload: str):
        await self.connection.lpush(self.ready_key, payload)

    def attach_consumer(self, consumer):
        async def wrapper():
            await self.connection.sadd(self.connections_key, f'{self.tag}-{self.connection_uuid}')

            while True:
                payload = await self.connection.rpoplpush(self.ready_key, self.unacked_key)
                if payload is None:
                    continue

                delivery = Delivery(self.connection, self.unacked_key, self.rejected_key, payload)
                await consumer(delivery)

        async def update_heartbeat():
            while True:
                await self.connection.set(self.heartbeat_key, "1", self.heartbeat_duration)
                await asyncio.sleep(10)

        self.tasks.append(asyncio.create_task(wrapper()))
        self.tasks.append(asyncio.create_task(update_heartbeat()))

    def attach_batch_consumer(self, size: int, timeout: int, consumer):
        async def wrapper():
            await self.connection.sadd(self.connections_key, f'{self.tag}-{self.connection_uuid}')
            deliveries = []

            async def consume(deliveries):
                while True:
                    payload = await self.connection.rpoplpush(self.ready_key, self.unacked_key)
                    if payload is None:
                        continue

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

        async def update_heartbeat():
            while True:
                await self.connection.set(self.heartbeat_key, "1", self.heartbeat_duration)
                await asyncio.sleep(10)

        self.tasks.append(asyncio.create_task(wrapper()))
        self.tasks.append(asyncio.create_task(update_heartbeat()))
