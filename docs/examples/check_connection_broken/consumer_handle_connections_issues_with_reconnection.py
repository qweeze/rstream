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
connection_is_closed = False


async def consume():
    async def on_connection_closed(disconnection_info: OnClosedErrorInfo) -> None:
        print(
            "connection has been closed from stream: "
            + str(disconnection_info.streams)
            + " for reason: "
            + str(disconnection_info.reason)
        )

        global connection_is_closed

        for stream in disconnection_info.streams:
            # restart from last offset in subscriber
            # alternatively you can specify an offset to reconnect
            await consumer.reconnect_stream(stream)

    consumer = Consumer(
        host="localhost",
        port=5552,
        vhost="/",
        username="guest",
        password="guest",
        on_close_handler=on_connection_closed,
    )

    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGINT, lambda: asyncio.create_task(consumer.close()))

    async def on_message(msg: AMQPMessage, message_context: MessageContext):
        stream = message_context.consumer.get_stream(message_context.subscriber_name)
        offset = message_context.offset
        global COUNT
        COUNT = COUNT + 1
        if COUNT % 1000000 == 0:
            # print("Got message: {} from stream {}, offset {}".format(msg, stream, offset))
            print("consumed 1 million messages")

    await consumer.start()
    await consumer.subscribe(
        stream=STREAM, callback=on_message, decoder=amqp_decoder, subscriber_name="subscriber_1"
    )
    await consumer.run()


asyncio.run(consume())
