import asyncio
import time

from rstream import AMQPMessage, OnClosedErrorInfo, Producer

STREAM = "my-test-stream"
MESSAGES = 10000000
connection_is_closed = False


async def publish():
    async def on_connection_closed(disconnection_info: OnClosedErrorInfo) -> None:
        print(
            "connection has been closed from stream: "
            + str(disconnection_info.streams)
            + " for reason: "
            + str(disconnection_info.reason)
        )

        for stream in disconnection_info.streams:
            print("reconnecting stream: " + stream)
            await producer.reconnect_stream(stream)

    print("creating Producer")
    # producer will be closed at the end by the async context manager
    # both if connection is still alive or not
    async with Producer(
        "localhost", username="guest", password="guest", on_close_handler=on_connection_closed
    ) as producer:

        # create a stream if it doesn't already exist
        await producer.create_stream(STREAM, exists_ok=True)

        # sending a million of messages in AMQP format
        start_time = time.perf_counter()

        print("Sending MESSAGES")
        for i in range(MESSAGES):
            amqp_message = AMQPMessage(
                body="hello: {}".format(i),
            )
            # send is asynchronous
            try:
                await producer.send(stream=STREAM, message=amqp_message)
            except:
                # wait for reconnection to happen
                await asyncio.sleep(2)
                continue

            if i % 10000 == 0:
                print("sent 10000 messages")

    end_time = time.perf_counter()
    print(f"Sent {MESSAGES} messages in {end_time - start_time:0.4f} seconds")


asyncio.run(publish())
