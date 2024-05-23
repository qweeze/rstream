import asyncio
import time

from rstream import AMQPMessage, Producer

STREAM = "my-test-stream"
LOOP = 10000
BATCH = 100


async def publish():
    async with Producer("localhost", username="guest", password="guest") as producer:
        # create a stream if it doesn't already exist
        await producer.create_stream(STREAM, exists_ok=True)

        start_time = time.perf_counter()

        # sending a million of messages in AMQP format
        for j in range(LOOP):
            messages = []
            for i in range(BATCH):
                amqp_message = AMQPMessage(
                    body=bytes("hello: {}".format(i), "utf-8"),
                )
                messages.append(amqp_message)
            # send_batch is synchronous. will wait till termination
            await producer.send_batch(stream=STREAM, batch=messages)

        end_time = time.perf_counter()
        message_for_second = LOOP * BATCH / (end_time - start_time)
        print(
            f"Sent {LOOP * BATCH} messages in {end_time - start_time:0.4f} seconds. "
            f"{message_for_second:0.4f} messages per second"
        )


asyncio.run(publish())
