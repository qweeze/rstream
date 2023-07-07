# RabbitMQ Stream Python Client

A Python asyncio-based client for [RabbitMQ Streams](https://github.com/rabbitmq/rabbitmq-server/tree/master/deps/rabbitmq_stream)


## Install

```bash
pip install rstream
```

## Quick start

### Publishing messages: 

You can publish messages with four different methods:

* send: asynchronous, messages are automatically buffered internally and sent at once after a timeout expires.
* send_batch: synchronous, the user buffers the messages and sends them. This is the fastest publishing method.
* send_wait: synchronous, the caller wait till the message is confirmed. This is the slowest publishing method.
* send_sub_entry: asynchronous, allow batch in sub-entry mode. This mode increases throughput at the cost of increased latency and potential duplicated messages even when deduplication is enabled. It also allows using compression to reduce bandwidth and storage if messages are reasonably similar, at the cost of increasing CPU usage on the client side.

Example Using send:

[producer using send](https://github.com/qweeze/rstream/blob/master/docs/examples/basic_producers/producer_send.py)

send is not thread safe so it must be awaited.

Similarly with the send_wait:

[producer using send_wait](https://github.com/qweeze/rstream/blob/master/docs/examples/basic_producers/producer_send_wait.py)

or using send_batch:

[producer using send_batch](https://github.com/qweeze/rstream/blob/master/docs/examples/basic_producers/producer_send_batch.py)

and eventually using sub_entry_batch:

[producer using sub_entry_batch](https://github.com/qweeze/rstream/blob/master/docs/examples/sub_entry_batch/producer_sub_entry_batch.py)


### Publishing with confirmation

The Send method takes as parameter an handle function that will be called asynchronously when the message sent will be notified from the server to have been published.

In this case the example will work like this:

[producer handling confirmation](https://github.com/qweeze/rstream/blob/master/docs/examples/producers_with_confirmations/send_with_confirmation.py)

Same is valid also for send_batch.

With `send_wait` instead will wait until the confirmation from the server is received.

### Consuming messages:

See examples here for basic consumer and consumers with different offsets:

[consumer examples](https://github.com/qweeze/rstream/blob/master/docs/examples/basic_consumers)

### Server-side offset tracking

RabbitMQ Streams provides server-side offset tracking for consumers. This features allows a consuming application to restart consuming where it left off in a previous run.
You can use the store_offset (to store an offset in the server) and query_offset (to query it) methods of the consumer class like in this example:

[server side offset tracking](https://github.com/qweeze/rstream/blob/master/docs/examples/manual_server_offset_tracking/consumer.py)

### Superstreams

The client is also supporting superstream: https://blog.rabbitmq.com/posts/2022/07/rabbitmq-3-11-feature-preview-super-streams/
A super stream is a logical stream made of individual, regular streams. It is a way to scale out publishing and consuming with RabbitMQ Streams: a large logical stream is divided into partition streams, splitting up the storage and the traffic on several cluster nodes.

You can use superstream_producer and superstream_consumer classes which internally uses producers and consumers to operate on the componsing streams.

How to create a superstream:

```
rabbitmq-streams add_super_stream orders  --routing-keys key1, key2,key3
```

How to send a message to a superstream:

[superstream producer](https://github.com/qweeze/rstream/blob/master/docs/examples/super_stream/super_stream_producer.py)

How to consume from a superstream:

[superstream consumer](https://github.com/qweeze/rstream/blob/master/docs/examples/super_stream/super_stream_consumer.py)

### Single Active Consumer support:

Single active consumer form streams is also supported: https://blog.rabbitmq.com/posts/2022/07/rabbitmq-3-11-feature-preview-single-active-consumer-for-streams/
See examples in: 

[single active consumer](https://github.com/qweeze/rstream/blob/master/docs/examples/single_active_consumer/single_active_consumer.py)

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
