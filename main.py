from typing import List
import time
import threading
import redis
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

    def ack(self):
        self.connection.lrem(self.unacked_key, 1, self.payload)

    def reject(self):
        self.connection.lpush(self.rejected_key, self.payload)


class Deliveries(object):

    def __init__(self, deliveries: List[Delivery]):
        self.deliveries = deliveries

    def ack(self):
        for delivery in self.deliveries:
            delivery.ack()

    def reject(self):
        for delivery in self.deliveries:
            delivery.reject()


class Queue(object):

    def __init__(self, connection: redis.Redis, queue_name: str):
        self.connection = connection
        self.queue_name = queue_name

        connection_uuid = uuid.uuid4()
        self.ready_key = f'rmq::queue::[{queue_name}]::ready'
        self.rejected_key = f'rmq::queue::[{queue_name}]::rejected'
        self.unacked_key = f'rmq::connection::[{connection_uuid}]::queue::[{queue_name}]::unacked'

        self.threads = []

    def wait(self):
        for thread in self.threads:
            thread.join()

    def publish(self, payload: str):
        self.connection.lpush(self.ready_key, payload)

    def attach_consumer(self, consumer):
        def wrapped():
            while True:
                payload = self.connection.rpoplpush(self.ready_key, self.unacked_key)
                if payload is None:
                    time.sleep(1)
                    continue
                delivery = Delivery(self.connection, self.unacked_key, self.rejected_key, payload)
                consumer(delivery)

        thread = threading.Thread(target=wrapped)
        thread.run()
        self.threads.append(thread)

    def attach_batch_consumer(self, size: int, timeout: int, consumer):
        def wrapped():
            deliveries = []
            tick = time.time()
            while True:
                payload = self.connection.rpoplpush(self.ready_key, self.unacked_key)

                if payload is None and time.time() - tick < timeout:
                    time.sleep(0.1)
                    continue

                if payload is not None:
                    delivery = Delivery(self.connection, self.unacked_key, self.rejected_key, payload)
                    deliveries.append(delivery)

                if len(deliveries) >= size or time.time() - tick > timeout:
                    consumer(Deliveries(deliveries))
                    deliveries = []
                    tick = time.time()

        thread = threading.Thread(target=wrapped)
        thread.run()
        self.threads.append(thread)


r = redis.Redis(host='localhost', port=6379, decode_responses=True)
queue_name = 'foobar'

queue = Queue(r, queue_name)

queue.publish('a')
queue.publish('b')
queue.publish('c')


def batch_consumer(delivery: Deliveries):
    print(len(delivery.deliveries))
    delivery.ack()


def consumer(delivery: Delivery):
    print(delivery.payload)
    delivery.ack()


# queue.attach_consumer(consumer)
queue.attach_batch_consumer(10000000000000000000, 1, batch_consumer)

queue.wait()

# publish('a')
# publish('b')
# publish('c')
# delivery = consume()
# print(delivery.payload)
# delivery.ack()
