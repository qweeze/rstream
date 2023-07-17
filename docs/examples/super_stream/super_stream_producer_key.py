import asyncio
import time

from rstream import (
    AMQPMessage,
    RouteType,
    SuperStreamProducer,
)

SUPER_STREAM = "invoices"
MESSAGES = 1000000

# routing key and the bindings between the super stream exchange and the streams
async def routing_extractor(message: AMQPMessage) -> str:
    return "key1"


async def publish():
    # SuperStreamProducer wraps a Producer
    async with SuperStreamProducer(
        "localhost",
        username="guest",
        password="guest",
        routing_extractor=routing_extractor,
        routing=RouteType.Key,
        super_stream=SUPER_STREAM,
    ) as super_stream_producer:
        start_time = time.perf_counter()
        # Sending a million messages
        for i in range(MESSAGES):
            amqp_message = AMQPMessage(
                body="hello: {}".format(i),
                application_properties={"id": "{}".format(i)},
            )
            await super_stream_producer.send(amqp_message)
        print("Published 1000000 messages to super stream: invoices")
        end_time = time.perf_counter()
        print(f"Sent {MESSAGES} messages in {end_time - start_time:0.4f} seconds")


asyncio.run(publish())
