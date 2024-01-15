import asyncio
import time

from rstream import AMQPMessage, OnClosedErrorInfo, Producer

STREAM = "my-test-stream"
MESSAGES = 10000000
producer_closed = False


async def publish():
    async def on_metadata_update(on_closed_info: OnClosedErrorInfo) -> None:

        if on_closed_info.reason == "MetaData Update":
            print(
                "metadata changed for stream : "
                + str(on_closed_info.streams[0])
                + " with code: "
                + on_closed_info.reason
            )

        else:
            print(
                "connection has been closed from stream: "
                + str(on_closed_info.streams)
                + " for reason: "
                + str(on_closed_info.reason)
            )

        global producer_closed
        producer_closed = True
        await asyncio.sleep(2)
        # reconnect if the stream is still existing
        if await producer.stream_exists(on_closed_info.streams[0]):
            await producer.reconnect_stream(on_closed_info.streams[0])
        else:
            await producer.close()

    print("creating Producer")
    # producer will be closed at the end by the async context manager
    # both if connection is still alive or not
    async with Producer(
        "localhost",
        username="guest",
        password="guest",
        on_close_handler=on_metadata_update,
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
            global producer_closed
            if producer_closed is False:
                try:
                    await producer.send(stream=STREAM, message=amqp_message)
                except Exception as e:
                    # gives time to reconnect_stream to reconnect
                    await asyncio.sleep(3)
                    continue
            else:
                producer_closed = False
                break

            if i % 100000 == 0:
                print("sent 10000 messages")

    end_time = time.perf_counter()
    print(f"Sent {MESSAGES} messages in {end_time - start_time:0.4f} seconds")


asyncio.run(publish())
