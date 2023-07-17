import asyncio
import time

from rstream import (
    AMQPMessage,
    RouteType,
    SuperStreamProducer,
)

SUPER_STREAM = "invoices"
MESSAGES = 1000000


# this value will be hashed using mumh3 hashing algorithm to decide the partition resolution for the message
async def routing_extractor(message: AMQPMessage) -> str:
    return message.application_properties["id"]


async def publish():
    # SuperStreamProducer wraps a Producer
    async with SuperStreamProducer(
        "localhost",
        username="guest",
        password="guest",
        routing_extractor=routing_extractor,
        routing=RouteType.Hash,
        super_stream=SUPER_STREAM,
    ) as super_stream_producer:
        # Sending a million messages

        start_time = time.perf_counter()
        for i in range(MESSAGES):
            amqp_message = AMQPMessage(
                body="hello: {}".format(i),
                application_properties={"id": "{}".format(i)},
            )
            await super_stream_producer.send(amqp_message)
            if i % 100000 == 0:
                print(f"Published {i} messages to super stream: {SUPER_STREAM}")

        end_time = time.perf_counter()
        print(
            f"Sent {MESSAGES} messages in {end_time - start_time:0.4f} seconds to super stream: {SUPER_STREAM}"
        )


asyncio.run(publish())
