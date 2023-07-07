import asyncio
import time

from rstream import AMQPMessage, Producer

STREAM = "my-test-stream"
MESSAGES = 1000000


async def publish():
    async with Producer("localhost", username="guest", password="guest") as producer:
        # create a stream if it doesn't already exist
        await producer.create_stream(STREAM, exists_ok=True)

        # sending a million of messages in binary format
        # note that this is not compatible with other clients (e.g. Java,.NET)
        # since they expect messages in AMQP 1.0
        start_time = time.perf_counter()

        for i in range(MESSAGES):
            # send is asynchronous
            await producer.send(stream=STREAM, message=b"hello")

        end_time = time.perf_counter()
        print(f"Sent {MESSAGES} messages in {end_time - start_time:0.4f} seconds")


asyncio.run(publish())
