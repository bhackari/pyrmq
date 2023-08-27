import redis.asyncio as redis


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
