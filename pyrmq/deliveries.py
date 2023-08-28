from typing import List
import redis.asyncio as redis

from .delivery import Delivery


class Deliveries(object):

    def __init__(self, client: redis.Redis, deliveries: List[Delivery]):
        self.client = client
        self.deliveries = deliveries

    async def ack(self):
        pipe = self.client.pipeline()
        pipe.multi()
        for delivery in self.deliveries:
            await pipe.lrem(delivery.unacked_key, 1, delivery.payload)
        return await pipe.execute()

    async def reject(self):
        pipe = self.client.pipeline()
        pipe.multi()
        for delivery in self.deliveries:
            await pipe.lrem(delivery.unacked_key, 1, delivery.payload)
        return await pipe.execute()
