import asyncio
import time

from rstream import AMQPMessage, CompressionType, Producer

STREAM = "my-test-stream"
LOOP = 5000
BATCH = 100


async def publish():
    async with Producer("localhost", username="guest", password="guest") as producer:
        await producer.create_stream(STREAM, exists_ok=True)
        start_time = time.perf_counter()

        print("Sending compressed and not compressed messages with sub_entry")

        for j in range(LOOP):
            messages = []
            for i in range(BATCH):
                amqp_message = AMQPMessage(
                    body="a:{}".format(i),
                )
                messages.append(amqp_message)

            # sending with compression
            await producer.send_sub_entry(
                STREAM, compression_type=CompressionType.Gzip, sub_entry_messages=messages
            )

            # sending without compression
            await producer.send_sub_entry(
                STREAM, compression_type=CompressionType.No, sub_entry_messages=messages
            )
        end_time = time.perf_counter()
        print(
            f"Sent {LOOP * BATCH * 2} messages in {end_time - start_time:0.4f} seconds {LOOP * BATCH} "
            f"compressed and {LOOP * BATCH} not compressed"
        )

    await producer.close()


asyncio.run(publish())
