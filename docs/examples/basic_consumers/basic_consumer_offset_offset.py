import asyncio
import signal

from rstream import (
    AMQPMessage,
    Consumer,
    ConsumerOffsetSpecification,
    MessageContext,
    OffsetType,
    amqp_decoder,
)

STREAM = "my-test-stream"


async def consume():
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
<<<<<<< HEAD
        stream = message_context.consumer.get_stream(message_context.subscriber_name)
        offset = message_context.offset
        print("Got message: {} from stream {}, offset {}".format(msg, stream, offset))
=======
        consumer = message_context.consumer
        stream = message_context.consumer.get_stream(message_context.subscriber_name)
        offset = message_context.offset

        print("Got message: {}".format(msg) + " from stream " + stream + " offset: " + str(offset))
>>>>>>> 55778554d17244679f955868c2f02b07514c7352

    await consumer.start()
    # Possible values of OffsetType are: FIRST (default), NEXT, LAST, TIMESTAMP and OFFSET
    await consumer.subscribe(
        stream=STREAM,
        callback=on_message,
        decoder=amqp_decoder,
        offset_specification=ConsumerOffsetSpecification(OffsetType.OFFSET, 10),
    )
    await consumer.run()


asyncio.run(consume())
