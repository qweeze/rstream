import asyncio
import signal

from rstream import (
    AMQPMessage,
    Consumer,
    FilterConfiguration,
    MessageContext,
    amqp_decoder,
)

STREAM = "my-test-stream"


async def consume():
    filters = []
    filters.append("1")

    consumer = Consumer(
        host="localhost",
        port=5552,
        vhost="/",
        username="guest",
        password="guest",
    )

    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGINT, lambda: asyncio.create_task(consumer.close()))

    async def on_message(msg: AMQPMessage, message_context: MessageContext):
        stream = message_context.consumer.get_stream(message_context.subscriber_name)
        offset = message_context.offset
        print("Got message: {} from stream {}, offset {}".format(msg, stream, offset))

    await consumer.start()
    await consumer.subscribe(
        stream=STREAM,
        callback=on_message,
        decoder=amqp_decoder,
        filter_input=FilterConfiguration(
            values_to_filter=filters,
            predicate=lambda message: message.application_properties[b"id"] == filters[0].encode("utf-8"),
            match_unfiltered=False,
        ),
    )
    await consumer.run()


asyncio.run(consume())
