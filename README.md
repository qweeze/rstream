# RabbitMQ Stream Python Client

A Python asyncio-based client for [RabbitMQ Streams](https://github.com/rabbitmq/rabbitmq-server/tree/master/deps/rabbitmq_stream)  
_This is a work in progress_

## Install

```bash
pip install rstream
```

## Quick start

Publishing messages:

```python
import asyncio
from rstream import Producer, AMQPMessage

async def publish():
    async with Producer('localhost', username='guest', password='guest') as producer:
        await producer.create_stream('mystream')

        for i in range(100):
            amqp_message = AMQPMessage(
                body='hello: {}'.format(i),
            )
            await producer.publish('mystream', amqp_message)

asyncio.run(publish())
```

Consuming messages:

```python
import asyncio
import signal
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
        print('Got message: {}'.format(msg.body))

    await consumer.start()
    await consumer.subscribe('mystream', on_message, decoder=amqp_decoder)
    await consumer.run()

asyncio.run(consume())
```

Connecting with SSL:

```python
import ssl

ssl_context = ssl.SSLContext()
ssl_context.load_cert_chain('/path/to/certificate.pem', '/path/to/key.pem')

producer = Producer(
    host='localhost',
    port=5551,
    ssl_context=ssl_context,
    username='guest',
    password='guest',
)
```

## TODO

- [ ] Documentation
- [ ] Handle `MetadataUpdate` and reconnect to another broker on stream configuration changes
- [ ] AsyncIterator protocol for consumer
- [ ] Add frame size validation
