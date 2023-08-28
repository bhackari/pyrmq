import redis.asyncio as redis
import asyncio
import random
import string

from .cleaner import Cleaner
from .deliveries import Deliveries
from .delivery import Delivery


# TODO(mirco): implement error handling in case of timeouts
class Queue(object):

    def __init__(self, client: redis.Redis, queue_name: str, tag: str):
        self.client = client
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
        self.cleaner = Cleaner(client, self.connections_key, self.ready_key, self.rejected_key, self.unacked_key)
        self.tasks.append(asyncio.create_task(self.cleaner.clean()))

    async def wait(self):
        return await asyncio.wait(self.tasks)

    async def publish(self, payload: str):
        return await self.client.lpush(self.ready_key, payload)

    def attach_consumer(self, consumer):
        async def wrapper():
            await self.client.sadd(self.connections_key, f'{self.tag}-{self.connection_uuid}')

            while True:
                payload = await self.client.rpoplpush(self.ready_key, self.unacked_key)
                if payload is None:
                    continue

                delivery = Delivery(self.client, self.unacked_key, self.rejected_key, payload)
                await consumer(delivery)

        async def update_heartbeat():
            while True:
                await self.client.set(self.heartbeat_key, "1", self.heartbeat_duration)
                await asyncio.sleep(10)

        self.tasks.append(asyncio.create_task(wrapper()))
        self.tasks.append(asyncio.create_task(update_heartbeat()))

    def attach_batch_consumer(self, size: int, timeout: int, consumer):
        async def wrapper():
            await self.client.sadd(self.connections_key, f'{self.tag}-{self.connection_uuid}')

            async def consume(deliveries):
                while len(deliveries) < size:
                    # TODO(mirco): we should pipeline this when batch sizes are big
                    payload = await self.client.brpoplpush(self.ready_key, self.unacked_key)
                    if payload is None:
                        continue

                    delivery = Delivery(self.client, self.unacked_key, self.rejected_key, payload)
                    deliveries.append(delivery)
                return

            while True:
                deliveries = []
                try:
                    await asyncio.wait_for(consume(deliveries), timeout)
                except asyncio.exceptions.TimeoutError:
                    pass
                finally:
                    if len(deliveries):
                        await consumer(Deliveries(self.client, deliveries))

        async def update_heartbeat():
            while True:
                await self.client.set(self.heartbeat_key, "1", self.heartbeat_duration)
                await asyncio.sleep(10)

        self.tasks.append(asyncio.create_task(wrapper()))
        self.tasks.append(asyncio.create_task(update_heartbeat()))
