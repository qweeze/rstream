import asyncio
import signal

from rstream import (
    AMQPMessage,
    ConsumerOffsetSpecification,
    DisconnectionErrorInfo,
    MessageContext,
    OffsetType,
    SuperStreamConsumer,
    amqp_decoder,
)

count = 0


async def on_message(msg: AMQPMessage, message_context: MessageContext):
    global count
    count += 1
    if (count % 100000) == 0:
        stream = await message_context.consumer.stream(message_context.subscriber_name)
        offset = message_context.offset
        print("Received message: {} from stream: {} - message offset: {}".format(msg, stream, offset))


async def on_connection_closed(disconnection_info: DisconnectionErrorInfo) -> None:
    print(
        "connection has been closed from stream: "
        + str(disconnection_info.streams)
        + " for reason: "
        + str(disconnection_info.reason)
    )



async def consume():
    consumer = SuperStreamConsumer(
        host="localhost",
        port=5552,
        vhost="/",
        username="guest",
        password="guest",
        super_stream="invoices",
        connection_closed_handler=on_connection_closed,
    )

    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGINT, lambda: asyncio.create_task(consumer.close()))
    offset_specification = ConsumerOffsetSpecification(OffsetType.FIRST, None)
    await consumer.start()
    await consumer.subscribe(
        callback=on_message, decoder=amqp_decoder, offset_specification=offset_specification
    )
    await consumer.run()


asyncio.run(consume())
