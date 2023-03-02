# RabbitMQ Stream Python Client

A Python asyncio-based client for [RabbitMQ Streams](https://github.com/rabbitmq/rabbitmq-server/tree/master/deps/rabbitmq_stream)
_This is a work in progress_

## Install

```bash
pip install rstream
```

## Quick start

### Publishing messages: 

You can publish messages with three different methods:

* send: asynchronous, messages are automatically buffered internally and sent at once after a timeout expires.
* batch_send: Synchronous, the user buffers the messages and sends them. This is the fastest publishing method.
* send_wait: Synchronous, the caller wait till the message is confirmed. This is the slowest publishing method.

Example Using send:

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
            await producer.send('mystream', amqp_message)

asyncio.run(publish())
```

send is not thread safe so it must be awaited.

Similarly with the send_wait:

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
            await producer.send_wait('mystream', amqp_message)

asyncio.run(publish())
```

Eventually using batch_send:

```python
import asyncio
from rstream import Producer, AMQPMessage

async def publish():
    async with Producer('localhost', username='guest', password='guest') as producer:
        await producer.create_stream('mystream')
        list_messages = []

        for i in range(100):
            amqp_message = AMQPMessage(
                body='hello: {}'.format(i),
            )
            list_messages.append(amqp_message)

        await producer.send_batch('mystream',  list_messages) 

asyncio.run(publish())
```

### Consuming messages:

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

### Connecting with SSL:

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

## Load Balancer

In order to handle load balancers, you can use the `load_balancer_mode` parameter for producers and consumers. This will always attempt to create a connection via the load balancer, discarding connections that are inappropriate for the client type.

Producers must connect to the leader node, while consumers can connect to any, prioritizing replicas if available.

## TODO

- [ ] Documentation
- [ ] Handle `MetadataUpdate` and reconnect to another broker on stream configuration changes
- [ ] AsyncIterator protocol for consumer
- [ ] Add frame size validation
