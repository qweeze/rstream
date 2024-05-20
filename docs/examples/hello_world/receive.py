import asyncio
import signal

from rstream import AMQPMessage, Consumer, MessageContext

STREAM = "hello-python-stream"


async def consume():
    consumer = Consumer(host="localhost", username="guest", password="guest")

    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGINT, lambda: asyncio.create_task(consumer.close()))

    async def on_message(msg: AMQPMessage, message_context: MessageContext):
        stream = message_context.consumer.get_stream(message_context.subscriber_name)
        offset = message_context.offset
        print("Got message: {} from stream {}, offset {}".format(msg, stream, offset))

    await consumer.start()
    print("press control+c to terminate")
    await consumer.subscribe(stream=STREAM, callback=on_message)
    await consumer.run()


asyncio.run(consume())
