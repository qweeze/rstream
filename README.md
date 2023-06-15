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
* batch_send: synchronous, the user buffers the messages and sends them. This is the fastest publishing method.
* send_wait: synchronous, the caller wait till the message is confirmed. This is the slowest publishing method.
* send_sub_entry: asynchronous, allow batch in sub-entry mode. This mode increases throughput at the cost of increased latency and potential duplicated messages even when deduplication is enabled. It also allows using compression to reduce bandwidth and storage if messages are reasonably similar, at the cost of increasing CPU usage on the client side.

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

or using batch_send:

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

and eventually using sub_entry_batch:

```python
async def publish():
    async with Producer('localhost', username='guest', password='guest') as producer:
        await producer.delete_stream('mystream', missing_ok=True)
        await producer.create_stream('mystream', exists_ok=True)

        messages = []
        for i in range(10):
            amqp_message = AMQPMessage(
                body='a:{}'.format(i),
            )
            messages.append(amqp_message_list)

       
        await producer.send_sub_entry('mixing', compression_type=CompressionType.Gzip,
                                      sub_entry_messages=messages)
        
        
        await producer.send_sub_entry('mixing', compression_type=CompressionType.No,
                                      sub_entry_messages=messages)

    await producer.close()
  
asyncio.run(publish())
```

You have the possibility to specify NoCompression (compression_type=CompressionType.No) or Gzip (compression_type=CompressionType.Gzip).

### Publishing with confirmation

The Send method takes as parameter an handle function that will be called asynchronously when the message sent will be notified from the server to have been published.

In this case the example will work like this:


```python
import asyncio
from rstream import Producer, AMQPMessage, ConfirmationStatus

def _on_publish_confirm_client(confirmation: ConfirmationStatus) -> None:

     if confirmation.is_confirmed == True:
        print("message id: " + str(confirmation.message_id) + " is confirmed")
     else:
         print("message id: " + str(confirmation.message_id) + " is not confirmed")


async def publish():
    async with Producer('localhost', username='guest', password='guest') as producer:
        await producer.create_stream('mystream')

        for i in range(100):
            amqp_message = AMQPMessage(
                body='hello: {}'.format(i),
            )
            await producer.send('mystream', amqp_message, on_publish_confirm=_on_publish_confirm_client) 

asyncio.run(publish())
```

Same is valid also for send_batch.

Please note that the publish confirmation callbacks are internally managed by the client and they are triggered in the Producer class.
This means that when the Producer will terminate its scope and lifetime you will not be able to receive the remaining notifications if any.
Depending on your scenario, you could add a synchronization mechanism (like an asyncio condition) to wait till all the notifications 
have been received or you could use an asyncio.wait to give time for the callbacks to be invoked by the client.


With `send_wait` instead will wait until the confirmation from the server is received.

### Consuming messages:

```python
import asyncio
import signal
from rstream import Consumer, amqp_decoder, AMQPMessage, MessageContext

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

    def on_message(msg: AMQPMessage, message_context: MessageContext):
        print('Got message: {}'.format(msg) + "from stream " + message_context.stream+ "offset: " + str(message_context.offset))

    await consumer.start()
    await consumer.subscribe('mystream', on_message, decoder=amqp_decoder)
    await consumer.run()

asyncio.run(consume())
```

### Superstreams

The client is also supporting superstream: https://blog.rabbitmq.com/posts/2022/07/rabbitmq-3-11-feature-preview-super-streams/
A super stream is a logical stream made of individual, regular streams. It is a way to scale out publishing and consuming with RabbitMQ Streams: a large logical stream is divided into partition streams, splitting up the storage and the traffic on several cluster nodes.

You can use superstream_producer and superstream_consumer classes which internally uses producers and consumers to operate on the componsing streams.

How to create a superstream:

```
rabbitmq-streams add_super_stream orders  --routing-keys key1, key2,key3
```

How to send a message to a superstream

```python
import asyncio
import time
import uamqp

from rstream import Producer, AMQPMessage, ConfirmationStatus, CompressionType, SuperStreamProducer, RouteType

async def routing_extractor(message: AMQPMessage) -> str:
    return str(message.properties.message_id)

async def publish():
    global counter
    counter = 0
    sent = 0
    async with SuperStreamProducer('localhost', username='guest', password='guest', super_stream='mixing', routing=RouteType.Hash, routing_extractor=routing_extractor) as producer:

        messages = []
        for i in range(1000):
            amqp_message = AMQPMessage(
                body='a:{}'.format(i),
                properties=uamqp.message.MessageProperties(message_id=i),
               
            )
           
            await producer.send(message=amqp_message, on_publish_confirm=_on_publish_confirm_client)

    
        await asyncio.sleep(1)
asyncio.run(publish())
```

How to consume from a superstream:

```python
import asyncio
import signal
from rstream import Consumer, amqp_decoder, AMQPMessage, SuperStreamConsumer, OffsetType, MessageContext
from typing import Optional

def on_message(msg: AMQPMessage, message_context: Optional[MessageContext]):
    print('Got message: {}'.format(msg) + "from stream " + message_context.stream+ "offset: " + str(message_context.offset))
        
async def consume():
    print("consume")
    consumer = SuperStreamConsumer(
        host='localhost',
        port=5552,
        vhost='/',
        username='guest',
        password='guest',
        super_stream='mixing'
    )

    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGINT, lambda: asyncio.create_task(consumer.close()))

    await consumer.start()
    await consumer.subscribe(callback=on_message, decoder=amqp_decoder, offset_type=OffsetType.FIRST)
    await consumer.run()

# main coroutine
async def main():
    # schedule the task
    task = asyncio.create_task(consume())
    # suspend a moment
      # wait a moment
    await asyncio.sleep(3)
    # cancel the task
    was_cancelled = task.cancel()
 
# run the asyncio program
asyncio.run(main())
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
