import asyncio
import signal
import time

from rstream import AMQPMessage, Producer, ConsumerOffsetSpecification, OffsetType

from rstream import (
    Consumer,
    MessageContext,
    amqp_decoder,
)

STREAM = "my-test-stream"
MESSAGES = 100


async def publish():
    async with Producer("localhost", username="guest", password="guest") as producer:
        # create a stream if it doesn't already exist
        await producer.create_stream(STREAM, exists_ok=True)

        # sending a million of messages in AMQP format
        start_time = time.perf_counter()

        for i in range(MESSAGES):
            amqp_message = AMQPMessage(
                body="hello: {}".format(i),
            )
            # send is asynchronous
            await producer.send(stream=STREAM, message=amqp_message)

        end_time = time.perf_counter()
        print(f"Sent {MESSAGES} messages in {end_time - start_time:0.4f} seconds")


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
        stream = message_context.consumer.get_stream(message_context.subscriber_name)
        offset = message_context.offset
        print("Got message: {} from stream {}, offset {}".format(msg, stream, offset))

    await consumer.start()
    await consumer.subscribe(stream=STREAM,
                             callback=on_message,
                             decoder=amqp_decoder,
                             offset_specification=ConsumerOffsetSpecification(OffsetType.OFFSET, 345))
    await consumer.run()


async def main():
    await publish()
    await consume()


asyncio.run(main())
