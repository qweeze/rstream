import asyncio
import time

from rstream import (
    AMQPMessage,
    DisconnectionErrorInfo,
    Producer,
)

STREAM = "my-test-stream"
MESSAGES = 1000000


async def on_connection_closed(disconnection_info: DisconnectionErrorInfo) -> None:
    print(
        "connection has been closed from stream: "
        + str(disconnection_info.streams)
        + " for reason: "
        + str(disconnection_info.reason)
    )


async def publish():

    async with Producer(
        "localhost", username="guest", password="guest", connection_closed_handler=on_connection_closed
    ) as producer:
        # create a stream if it doesn't already exist
        await producer.create_stream(STREAM, exists_ok=True)

        # sending a million of messages in AMQP format
        start_time = time.perf_counter()

        for i in range(MESSAGES):
            amqp_message = AMQPMessage(
                body="hello: {}".format(i),
            )
            # send is asynchronous
            await producer.send(stream=STREAM, message=amqp_message)

        end_time = time.perf_counter()
        print(f"Sent {MESSAGES} messages in {end_time - start_time:0.4f} seconds")


asyncio.run(publish())
