import asyncio
import signal
from collections import defaultdict

from rstream import (
    AMQPMessage,
    EventContext,
    MessageContext,
    OffsetSpecification,
    OffsetType,
    SuperStreamConsumer,
    amqp_decoder,
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


# We can decide a strategy to manage Offset specification in single active consumer based on is_active flag
# By default if not present the always the strategy OffsetType.NEXT will be set.
# This handle will be passed to subscribe.
def consumer_update_handler_offset(is_active: bool, event_context: EventContext) -> OffsetSpecification:

    print("consumer_update_handler_offset enabled:")
    print("on stream: " + str(event_context.consumer.get_stream(event_context.subscriber_name)))
    print("reference group: " + str(event_context.reference))
    return OffsetSpecification(OffsetType.OFFSET, 10)


async def consume():
    try:

        print("Starting Super Stream Consumer")
        consumer = SuperStreamConsumer(
            host="localhost", port=5552, vhost="/", username="guest", password="guest", super_stream="mixing"
        )

        loop = asyncio.get_event_loop()
        loop.add_signal_handler(signal.SIGINT, lambda: asyncio.create_task(consumer.close()))

        await consumer.start()

        # properties of the consumer (enabling single active mode)
        properties: dict[str, str] = defaultdict(str)
        properties["single-active-consumer"] = "true"
        properties["name"] = "consumer-group-1"
        properties["super-stream"] = "mixing"

        offset_specification = OffsetSpecification(OffsetType.FIRST, None)

        await consumer.subscribe(
            callback=on_message,
            offset_specification=offset_specification,
            decoder=amqp_decoder,
            properties=properties,
            consumer_update_handler=consumer_update_handler_offset,
        )
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
