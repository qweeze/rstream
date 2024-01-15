import asyncio
import signal

from rstream import (
    AMQPMessage,
    Consumer,
    MessageContext,
    OnClosedErrorInfo,
    amqp_decoder,
)

STREAM = "my-test-stream"
COUNT = 0
consumer_closed = False


async def print_count_values():

    global COUNT

    while True:
        await asyncio.sleep(5)

        print("NUMBER OF CONSUMED MESSAGES:         " + str(COUNT))


async def consume():
    asyncio.create_task(print_count_values())

    async def connect() -> Consumer:
        async def on_metadata_update(on_closed_info: OnClosedErrorInfo) -> None:
            if on_closed_info.reason == "MetaData Update":
                print(
                    "metadata changed for stream : "
                    + str(on_closed_info.streams[0])
                    + " with code: "
                    + on_closed_info.reason
                )
                await asyncio.sleep(2)
                # reconnect just if the stream exists
                if await consumer.stream_exists(on_closed_info.streams[0]):
                    await consumer.reconnect_stream(on_closed_info.streams[0])
                else:
                    await consumer.close()

            else:
                print(
                    "connection has been closed from stream: "
                    + str(on_closed_info.streams)
                    + " for reason: "
                    + str(on_closed_info.reason)
                )
                await asyncio.sleep(2)
                # reconnect just if the stream exists
                for stream in on_closed_info.streams:
                    await consumer.reconnect_stream(stream)

        consumer = Consumer(
            host="localhost",
            port=5552,
            vhost="/",
            username="guest",
            password="guest",
            on_close_handler=on_metadata_update,
        )

        return consumer

    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGINT, lambda: asyncio.create_task(consumer.close()))

    async def on_message(msg: AMQPMessage, message_context: MessageContext):
        stream = message_context.consumer.get_stream(message_context.subscriber_name)
        offset = message_context.offset
        global COUNT
        COUNT = COUNT + 1

    consumer = await connect()
    await consumer.start()
    await consumer.subscribe(stream=STREAM, callback=on_message, decoder=amqp_decoder)
    await consumer.run()


asyncio.run(consume())
