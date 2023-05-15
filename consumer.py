import asyncio
import signal
import time

from rstream import Consumer, amqp_decoder, AMQPMessage


async def consume():
    consumer = Consumer(
        host='localhost',
        port=5552,
        vhost='/',
        username='guest',
        password='guest',
    )

    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGINT, lambda: asyncio.create_task(consumer.close()))

    def on_message(msg: AMQPMessage):
        print('Got message: {}'.format(msg))
        pass

    await consumer.start()
    await consumer.subscribe('mystream', on_message, decoder=amqp_decoder)
    await consumer.run()


asyncio.run(consume())
