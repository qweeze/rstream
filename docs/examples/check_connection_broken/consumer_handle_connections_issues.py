import asyncio
import signal

from rstream import (
    AMQPMessage,
    Consumer,
    DisconnectionErrorInfo,
    MessageContext,
    amqp_decoder,
)

STREAM = "my-test-stream"


async def on_connection_closed(disconnection_info: DisconnectionErrorInfo) -> None:
    print(
        "connection has been closed from stream: "
        + str(disconnection_info.streams)
        + " for reason: "
        + str(disconnection_info.reason)
    )


async def consume():
    consumer = Consumer(
        host="localhost",
        port=5552,
        vhost="/",
        username="guest",
        password="guest",
        connection_closed_handler=on_connection_closed,
    )

    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGINT, lambda: asyncio.create_task(consumer.close()))

    async def on_message(msg: AMQPMessage, message_context: MessageContext):
        stream = message_context.consumer.get_stream(message_context.subscriber_name)
        offset = message_context.offset
        print("Got message: {} from stream {}, offset {}".format(msg, stream, offset))

    await consumer.start()
    await consumer.subscribe(stream=STREAM, callback=on_message, decoder=amqp_decoder)
    await consumer.run()


asyncio.run(consume())
