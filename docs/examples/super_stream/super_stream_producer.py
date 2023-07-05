import asyncio
import time

from rstream import (
    AMQPMessage,
    RouteType,
    SuperStreamProducer,
)

SUPER_STREAM = "invoices"


async def routing_extractor(message: AMQPMessage) -> str:
    return message.application_properties["id"]


async def publish():
    async with SuperStreamProducer(
        "localhost",
        username="guest",
        password="guest",
        routing_extractor=routing_extractor,
        routing=RouteType.Hash,
        super_stream=SUPER_STREAM,
    ) as super_stream_producer:
        for i in range(1000000):
            amqp_message = AMQPMessage(
                body="hello: {}".format(i),
                application_properties={"id": "{}".format(i)},
            )
            await super_stream_producer.send(amqp_message)
        print("Published 1000000 messages to super stream: invoices")


asyncio.run(publish())
