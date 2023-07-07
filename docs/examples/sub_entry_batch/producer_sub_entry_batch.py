import asyncio
import time

from rstream import AMQPMessage, CompressionType, Producer

STREAM = "my-test-stream"
MESSAGES = 100


async def publish():
    async with Producer("localhost", username="guest", password="guest") as producer:
        await producer.create_stream(STREAM, exists_ok=True)

        messages = []
        for i in range(MESSAGES):
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

    await producer.close()


asyncio.run(publish())
