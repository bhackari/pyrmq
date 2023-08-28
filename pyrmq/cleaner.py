import redis.asyncio as redis
import asyncio


class Cleaner(object):

    def __init__(self,
                 client: redis.Redis,
                 connections_key: str,
                 ready_key: str,
                 rejected_key: str,
                 unacked_key: str,
                 ):
        self.client = client
        self.connections_key = connections_key
        self.ready_key = ready_key
        self.rejected_key = rejected_key
        self.unacked_key = unacked_key

    async def clean(self):
        while True:
            # connections = await self.client.smembers(self.connections_key)
            # for connection in connections:
            #     if await self.client.ttl(f'rmq::connection::{connection}::heartbeat') < 0:
            #         pass

            await asyncio.gather(
                self.client.rpoplpush(self.unacked_key, self.ready_key),
                self.client.rpoplpush(self.rejected_key, self.ready_key),
            )

            await asyncio.sleep(10)
