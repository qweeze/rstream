import asyncio

from rstream import (
    AMQPMessage,
    RouteType,
    SuperStreamProducer,
)

SUPER_STREAM = "invoices"
MESSAGES = 200


# this value will be hashed using mumh3 hashing algorithm to decide the partition resolution for the message
async def routing_extractor(message: AMQPMessage) -> str:
    return message.application_properties["id"]


async def filter_value_extractor(message: AMQPMessage) -> str:
    return message.application_properties["region"]


async def publish():
    # SuperStreamProducer wraps a Producer
    async with SuperStreamProducer(
        "localhost",
        username="guest",
        password="guest",
        routing_extractor=routing_extractor,
        routing=RouteType.Hash,
        super_stream=SUPER_STREAM,
        filter_value_extractor=filter_value_extractor,
    ) as super_stream_producer:
        # Sending a million messages

        # sending 200 messages with filtering New York
        for i in range(MESSAGES):
            application_properties = {"region": "New York", "id": "{}".format(i)}
            amqp_message = AMQPMessage(
                body=bytes("hello: {}".format(i), "utf-8"),
                application_properties=application_properties,
            )
            # send is asynchronous
            await super_stream_producer.send(message=amqp_message)

        # wait a bit to ensure all messages will go to a chunk
        # don't do this in production. This is only for testing purposes
        # it is to force the bloom filter to be created
        await asyncio.sleep(2)

        # sending 200 messages with filtering California
        for i in range(MESSAGES):
            application_properties = {"region": "California", "id": "{}".format(i)}
            amqp_message = AMQPMessage(
                body=bytes("hello: {}".format(i), "utf-8"),
                application_properties=application_properties,
            )
            # send is asynchronous
            await super_stream_producer.send(message=amqp_message)


asyncio.run(publish())
