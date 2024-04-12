import asyncio
import time

from rstream import AMQPMessage, Producer, RawMessage

STREAM = "my-test-stream"
LOOP_1 = 1000
LOOP_2 = 1000


async def publish():
    async with Producer("localhost", username="guest", password="guest") as producer:
        # create a stream if it doesn't already exist
        await producer.create_stream(STREAM, exists_ok=True)

        # sending a million of messages in AMQP format
        start_time = time.perf_counter()

        for j in range(LOOP_1):
            for i in range(LOOP_2):
                await producer.send(
                    STREAM,
                    # just 1000 messages will be inserted as messages with the same publishing_id and publisher_name will be discarded
                    RawMessage(f"test_{j}".encode(), publshing_id=j),
                )

        end_time = time.perf_counter()
        print(f"Sent {LOOP_1} messages in {end_time - start_time:0.4f} seconds")


asyncio.run(publish())
