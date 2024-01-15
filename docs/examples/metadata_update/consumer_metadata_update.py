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


async def consume():
    async def connect() -> Consumer:
        async def on_metadata_update(metadata_update_info: OnClosedErrorInfo) -> None:
            print(
                "metadata changed for stream : "
                + str(metadata_update_info.streams[0])
                + " with code: "
                + metadata_update_info.reason
            )

            await asyncio.sleep(2)
            # reconnect just if the stream exists
            if await consumer.stream_exists(metadata_update_info.streams[0]):
                await consumer.reconnect_stream(metadata_update_info.streams[0])
            else:
                await consumer.close()

        consumer = Consumer(
            host="34.89.82.143",
            port=5552,
            vhost="/",
            username="XXXXXXXXXXXXX",
            password="XXXXXXXXXXXXX",
            load_balancer_mode=True,
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
        if COUNT % 100000 == 0:
            print("Got message: {} from stream {}, offset {}".format(msg, stream, offset))
            print("consumed 1 million messages")

    consumer = await connect()
    await consumer.start()
    await consumer.subscribe(stream=STREAM, callback=on_message, decoder=amqp_decoder)
    await consumer.run()


asyncio.run(consume())
