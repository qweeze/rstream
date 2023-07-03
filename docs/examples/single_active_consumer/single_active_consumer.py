import asyncio
import signal
from collections import defaultdict

from rstream import (
    AMQPMessage,
    MessageContext,
    OffsetType,
    SuperStreamConsumer,
    amqp_decoder, OffsetSpecification,
)

cont = 0
lock = asyncio.Lock()


async def on_message(msg: AMQPMessage, message_context: MessageContext):
    global cont
    global lock

    consumer = message_context.consumer
    stream = await message_context.consumer.stream(message_context.subscriber_name)
    offset = message_context.offset

    print("Got message: {} from stream {} offset {}".format(msg, stream, offset))


async def consume():
    try:

        print("Starting Super Stream Consumer")
        consumer = SuperStreamConsumer(
            host="localhost", port=5552, vhost="/", username="guest", password="guest", super_stream="invoices"
        )

        loop = asyncio.get_event_loop()
        loop.add_signal_handler(signal.SIGINT, lambda: asyncio.create_task(consumer.close()))

        await consumer.start()

        # properties of the consumer (enabling single active mode)
        properties: dict[str, str] = defaultdict(str)
        properties["single-active-consumer"] = "true"
        properties["name"] = "consumer-group-1"
        properties["super-stream"] = "invoices"
        print("Subscribing Super Stream Consumer")

        offset_specification = OffsetSpecification(OffsetType.FIRST, None)

        await consumer.subscribe(callback=on_message,
                                 offset_specification=offset_specification,
                                 decoder=amqp_decoder,
                                 properties=properties)
        await consumer.run()
    except Exception as e:
        print(e)


# main coroutine
async def main():
    # schedule the task
    task = asyncio.create_task(consume())
    # suspend a moment
    # wait a moment
    await asyncio.sleep(100)
    # cancel the task
    was_cancelled = task.cancel()

    # report a message
    print("Main done")


# run the asyncio program
asyncio.run(main())
