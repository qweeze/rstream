import asyncio
import time

from rstream import (
    AMQPMessage,
    DisconnectionErrorInfo,
    RouteType,
    SuperStreamProducer,
)

SUPER_STREAM = "test_super_stream"
MESSAGES = 10000000


async def publish():
    # this value will be hashed using mumh3 hashing algorithm to decide the partition resolution for the message
    async def routing_extractor(message: AMQPMessage) -> str:
        return message.application_properties["id"]

    async def on_connection_closed(disconnection_info: DisconnectionErrorInfo) -> None:

        print(
            "connection has been closed from stream: "
            + str(disconnection_info.streams)
            + " for reason: "
            + str(disconnection_info.reason)
        )

    async with SuperStreamProducer(
        "localhost",
        username="guest",
        password="guest",
        routing_extractor=routing_extractor,
        routing=RouteType.Hash,
        connection_closed_handler=on_connection_closed,
        super_stream=SUPER_STREAM,
    ) as super_stream_producer:

        # sending a million of messages in AMQP format
        start_time = time.perf_counter()

        for i in range(MESSAGES):
            amqp_message = AMQPMessage(
                body="hello: {}".format(i),
                application_properties={"id": "{}".format(i)},
            )
            # send is asynchronous
            await super_stream_producer.send(message=amqp_message)

        end_time = time.perf_counter()
        print(f"Sent {MESSAGES} messages in {end_time - start_time:0.4f} seconds")


asyncio.run(publish())
