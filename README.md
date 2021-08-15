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
from rstream import Producer, Consumer

async with Producer('localhost', username='guest', password='guest') as producer:
    await producer.create_stream('mystream')

    for i in range(100):
        await producer.publish('mystream', f'msg: {i}'.encode())

```

Consuming messages:

```python
consumer = Consumer(
    host='localhost',
    port=5552,
    vhost='/',
    username='guest',
    password='guest',
)

loop = asyncio.get_event_loop()
loop.add_signal_handler(signal.SIGINT, lambda: asyncio.create_task(consumer.close()))

def on_message(msg):
    print('Got message:', msg)

await consumer.start()
await consumer.subscribe('mystream', on_message)
await consumer.run()
```
