import asyncio
import signal
from typing import Optional

from rstream import (
    AMQPMessage,
    MessageContext,
    OffsetType,
    SuperStreamConsumer,
    amqp_decoder, ConsumerOffsetSpecification,
)

cont = 0


async def on_message(msg: AMQPMessage, message_context: MessageContext):
    stream = await message_context.consumer.stream(message_context.subscriber_name)
    offset = message_context.offset
    print("Received message: {} from stream: {} - message offset: {}".format(msg, stream, offset))


async def consume():
    consumer = SuperStreamConsumer(
        host="localhost", port=5552, vhost="/", username="guest", password="guest", super_stream="invoices"
    )

    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGINT, lambda: asyncio.create_task(consumer.close()))
    offset_specification = ConsumerOffsetSpecification(OffsetType.FIRST, None)
    await consumer.start()
    await consumer.subscribe(callback=on_message, decoder=amqp_decoder, offset_specification=offset_specification)
    await consumer.run()


asyncio.run(consume())
