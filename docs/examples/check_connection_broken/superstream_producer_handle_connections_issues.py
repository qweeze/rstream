import asyncio
import time

from rstream import (
    AMQPMessage,
    DisconnectionErrorInfo,
    RouteType,
    SuperStreamProducer,
)

SUPER_STREAM = "invoices"
MESSAGES = 10000000

connection_is_closed = False


async def publish():
    # this value will be hashed using mumh3 hashing algorithm to decide the partition resolution for the message
    async def routing_extractor(message: AMQPMessage) -> str:
        return message.application_properties["id"]

    async def on_connection_closed(disconnection_info: DisconnectionErrorInfo) -> None:

        print(
            "connection has been closed from stream: "
            + str(disconnection_info.streams)
            + " for reason: "
            + disconnection_info.reason
        )
        global connection_is_closed
        connection_is_closed = True

        # avoid multiple simultaneous disconnection to call close multiple times
        if connection_is_closed is False:
            await super_stream_producer.close()
            connection_is_closed = True

    # avoiding using async context as we close the producer ourself in on_connection_closed callback
    super_stream_producer = SuperStreamProducer(
        "localhost",
        username="guest",
        password="guest",
        routing_extractor=routing_extractor,
        routing=RouteType.Hash,
        connection_closed_handler=on_connection_closed,
        super_stream=SUPER_STREAM,
    )

    await super_stream_producer.start()

    # sending a million of messages in AMQP format
    start_time = time.perf_counter()
    global connection_is_closed

    for i in range(MESSAGES):
        amqp_message = AMQPMessage(
            body="hello: {}".format(i),
            application_properties={"id": "{}".format(i)},
        )

        # send is asynchronous
        if connection_is_closed is False:
            await super_stream_producer.send(message=amqp_message)
        else:
            break

    if connection_is_closed is False:
        await super_stream_producer.close()

    end_time = time.perf_counter()
    print(f"Sent {MESSAGES} messages in {end_time - start_time:0.4f} seconds")


asyncio.run(publish())
