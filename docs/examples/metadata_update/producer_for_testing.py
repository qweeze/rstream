import asyncio
import time

from rstream import (
    AMQPMessage,
    ConfirmationStatus,
    OnClosedErrorInfo,
    Producer,
)

STREAM = "my-test-stream"
MESSAGES = 10000000
producer_closed = False
COUNT = 0
CONFIRMED = 0


async def _on_publish_confirm_client(confirmation: ConfirmationStatus) -> None:
    global CONFIRMED
    if confirmation.is_confirmed:
        CONFIRMED = CONFIRMED + 1


async def print_count_values():

    global COUNT
    global CONFIRMED

    while True:
        await asyncio.sleep(5)

        print("NUMBER OF PRODUCED MESSAGES:         " + str(COUNT))
        print("NUMBER OF CONFIRMED MESSAGES:        " + str(CONFIRMED))


async def publish():
    asyncio.create_task(print_count_values())

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
        await producer.reconnect_stream(on_closed_info.streams[0])

    print("creating Producer")
    # producer will be closed at the end by the async context manager
    # both if connection is still alive or not
    async with Producer(
        "localhost",
        username="guest",
        password="guest",
        load_balancer_mode=False,
        on_close_handler=on_metadata_update,
    ) as producer:

        # create a stream if it doesn't already exist
        await producer.create_stream(STREAM, exists_ok=True)

        # sending a million of messages in AMQP format
        start_time = time.perf_counter()

        print("Sending MESSAGES")
        global COUNT
        for i in range(MESSAGES):
            amqp_message = AMQPMessage(
                body="hello: {}".format(i),
            )
            # send is asynchronous
            global producer_closed
            if producer_closed is False:
                try:
                    await producer.send(
                        stream=STREAM, message=amqp_message, on_publish_confirm=_on_publish_confirm_client
                    )
                except Exception as e:
                    await asyncio.sleep(3)
                    continue
            else:
                producer_closed = False
                continue

            COUNT = COUNT + 1

    end_time = time.perf_counter()
    print(f"Sent {MESSAGES} messages in {end_time - start_time:0.4f} seconds")


asyncio.run(publish())
