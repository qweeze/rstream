import asyncio
import signal

from rstream import (
    AMQPMessage,
    ConsumerOffsetSpecification,
    MessageContext,
    OffsetType,
    OnClosedErrorInfo,
    SuperStreamConsumer,
    amqp_decoder,
)

count = 0
connection_is_closed = False


async def on_message(msg: AMQPMessage, message_context: MessageContext):
    global count
    count += 1
    if (count % 100000) == 0:
        stream = await message_context.consumer.stream(message_context.subscriber_name)
        offset = message_context.offset
        print("Received message: {} from stream: {} - message offset: {}".format(msg, stream, offset))


async def consume():
    async def on_metadata_update(on_closed_info: OnClosedErrorInfo) -> None:

        print(
            "connection has been closed from stream: "
            + str(on_closed_info.streams)
            + " for reason: "
            + str(on_closed_info.reason)
        )

        await asyncio.sleep(2)
        # reconnect just if the partition exists
        for stream in on_closed_info.streams:
            backoff = 1
            while True:
                try:
                    print("reconnecting stream: {}".format(stream))
                    await consumer.reconnect_stream(stream)
                    break
                except Exception as ex:
                    if backoff > 32:
                        # failed to found the leader
                        print("reconnection failed")
                        break
                    backoff = backoff * 2
                    print("exception reconnecting waiting 120s: " + str(ex))
                    await asyncio.sleep(30)
                    continue

    consumer = SuperStreamConsumer(
        host="test",
        port=5552,
        vhost="/",
        username="guest",
        password="guest",
        super_stream="invoices",
        on_close_handler=on_metadata_update,
        load_balancer_mode=True,
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
