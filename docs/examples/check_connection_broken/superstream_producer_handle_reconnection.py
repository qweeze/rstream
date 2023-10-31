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

        for stream in disconnection_info.streams:
            print("stream disconnected: " + stream)
            await super_stream_producer.reconnect_stream(stream)

    # super_stream_producer will be closed by the async context manager
    # both if connection is still alive or not
    print("creating super_stream producer")
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
        global connection_is_closed

        print("sending messages")
        for i in range(MESSAGES):
            amqp_message = AMQPMessage(
                body="hello: {}".format(i),
                application_properties={"id": "{}".format(i)},
            )

            # send is asynchronous
            try:
                await super_stream_producer.send(message=amqp_message)
            except:
                # wait for reconnection to happen
                await asyncio.sleep(2)
                continue

            if i % 10000 == 0:
                print("sent 10000 MESSAGES")

    end_time = time.perf_counter()
    print(f"Sent {MESSAGES} messages in {end_time - start_time:0.4f} seconds")


asyncio.run(publish())
