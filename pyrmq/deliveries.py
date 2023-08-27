from typing import List
import asyncio

from .delivery import Delivery


class Deliveries(object):

    def __init__(self, deliveries: List[Delivery]):
        self.deliveries = deliveries

    async def ack(self):
        return await asyncio.gather(*[delivery.ack() for delivery in self.deliveries])

    async def reject(self):
        return await asyncio.gather(*[delivery.reject() for delivery in self.deliveries])
