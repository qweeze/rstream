# RabbitMQ Stream Python Client

A Python asyncio-based client for [RabbitMQ Streams](https://github.com/rabbitmq/rabbitmq-server/tree/master/deps/rabbitmq_stream)


## Install

The client is distributed via [`PIP`](https://pypi.org/project/rstream/):
```bash
	pip install rstream
```


## Publishing messages: 

You can publish messages with four different methods:

* `send`: asynchronous, messages are automatically buffered internally and sent at once after a timeout expires.
* `send_batch`: synchronous, the user buffers the messages and sends them. This is the fastest publishing method.
* `send_wait`: synchronous, the caller wait till the message is confirmed. This is the slowest publishing method.
* `send_sub_entry`: asynchronous, allow batch in sub-entry mode. This mode increases throughput at the cost of increased latency and potential duplicated messages even when deduplication is enabled. It also allows using compression to reduce bandwidth and storage if messages are reasonably similar, at the cost of increasing CPU usage on the client side.



[Examples](https://github.com/qweeze/rstream/blob/master/docs/examples/):
- [producer using send](https://github.com/qweeze/rstream/blob/master/docs/examples/basic_producers/producer_send.py)
- [producer using send_wait](https://github.com/qweeze/rstream/blob/master/docs/examples/basic_producers/producer_send_wait.py)
- [producer using send_batch](https://github.com/qweeze/rstream/blob/master/docs/examples/basic_producers/producer_send_batch.py)
- [producer using sub_entry_batch](https://github.com/qweeze/rstream/blob/master/docs/examples/sub_entry_batch/producer_sub_entry_batch.py)


### Publishing with confirmation

The Send method takes as parameter an handle function that will be called asynchronously when the message sent will be notified from the server to have been published.

Example:
- [producer using send and handling confirmation](https://github.com/qweeze/rstream/blob/master/docs/examples/producers_with_confirmations/send_with_confirmation.py)
- [producer using send_batch and handling confirmation](https://github.com/qweeze/rstream/blob/master/docs/examples/producers_with_confirmations/send_batch_with_confirmation.py)

With `send_wait` instead will wait until the confirmation from the server is received.

### Consuming messages:

See [consumer examples](https://github.com/qweeze/rstream/blob/master/docs/examples/basic_consumers)  for basic consumer and consumers with different offsets.


### Server-side offset tracking

RabbitMQ Streams provides server-side offset tracking for consumers. This features allows a consuming application to restart consuming where it left off in a previous run.
You can use the store_offset (to store an offset in the server) and query_offset (to query it) methods of the consumer class like in this example:
- [server side offset tracking](https://github.com/qweeze/rstream/blob/master/docs/examples/manual_server_offset_tracking/consumer.py)


### Superstreams

A super stream is a logical stream made of individual, regular streams. It is a way to scale out publishing and consuming with RabbitMQ Streams: a large logical stream is divided into partition streams, splitting up the storage and the traffic on several cluster nodes.

See the [blog post](https://blog.rabbitmq.com/posts/2022/07/rabbitmq-3-11-feature-preview-super-streams/) for more info.

You can use `superstream_producer` and `superstream_consumer` classes which internally uses producers and consumers to operate on the componsing streams.


See the Super [Stream example](https://github.com/qweeze/rstream/tree/master/docs/examples/super_stream)

### Single Active Consumer support:

Single active consumer provides exclusive consumption and consumption continuity on a stream. <br /> 
See the [blog post](https://blog.rabbitmq.com/posts/2022/07/rabbitmq-3-11-feature-preview-single-active-consumer-for-streams) for more info.
See examples in: 

See the [single active consumer example](https://github.com/qweeze/rstream/blob/master/docs/examples/single_active_consumer/)

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
