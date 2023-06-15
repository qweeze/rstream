import asyncio
import signal
from typing import Optional

from rstream import (
    AMQPMessage,
    MessageContext,
    OffsetType,
    SuperStreamConsumer,
    amqp_decoder,
)

cont = 0


def on_message(msg: AMQPMessage, message_context: MessageContext):
    print(
        "Got message: {}".format(msg)
        + "from stream "
        + message_context.stream
        + "offset: "
        + str(message_context.offset)
    )


async def consume():
    print("consume")
    consumer = SuperStreamConsumer(
        host="localhost", port=5552, vhost="/", username="guest", password="guest", super_stream="mixing"
    )

    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGINT, lambda: asyncio.create_task(consumer.close()))

    await consumer.start()
    await consumer.subscribe(callback=on_message, decoder=amqp_decoder, offset_type=OffsetType.FIRST)
    await consumer.run()
