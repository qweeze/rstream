# RabbitMQ Stream Python Client

A Python asyncio-based client for [RabbitMQ Streams](https://github.com/rabbitmq/rabbitmq-server/tree/master/deps/rabbitmq_stream)


## Install

The client is distributed via [`PIP`](https://pypi.org/project/rstream/):
```bash
	pip install rstream
```

## Client Codecs
Before start using the client is important to read this section.
The client supports two codecs to store the messages to the server:
 - `AMQP 1.0`
 - `Binary`

By default you should use `AMQP 1.0` codec:
```python
   amqp_message = AMQPMessage(
    body="hello: {}".format(i),
  )
```
 
#### AMQP 1.0 codec  vs Binary

You need to use the `AMQP 1.0` codec to exchange messages with other stream clients like
[Java](https://github.com/rabbitmq/rabbitmq-stream-java-client), [.NET](https://github.com/rabbitmq/rabbitmq-stream-dotnet-client), [Rust](https://github.com/rabbitmq/rabbitmq-stream-rust-client), [Go](https://github.com/rabbitmq/rabbitmq-stream-go-client) or if you want to use the `AMQP 0.9.1` clients. 

You can use the `Binary` version if you need to exchange messages from Python to Python. 

<b>Note</b>: The messages stored in `Binary` are not compatible with the other clients and with AMQP 0.9.1 clients. <br /> 
Once the messages are stored to the server, you can't change them. 

Read also the [Client Performances](#client-performances) section 


## Publishing messages

You can publish messages with four different methods:

* `send`: asynchronous, messages are automatically buffered internally and sent at once after a timeout expires.
* `send_batch`: synchronous, the user buffers the messages and sends them. This is the fastest publishing method.
* `send_wait`: synchronous, the caller wait till the message is confirmed. This is the slowest publishing method.
* `send_sub_entry`: asynchronous, allow batch in sub-entry mode. This mode increases throughput at the cost of increased latency and potential duplicated messages even when deduplication is enabled. It also allows using compression to reduce bandwidth and storage if messages are reasonably similar, at the cost of increasing CPU usage on the client side.


On the [examples](https://github.com/qweeze/rstream/blob/master/docs/examples/) directory you can find diffent way to send the messages:
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

## Consuming messages

See [consumer examples](https://github.com/qweeze/rstream/blob/master/docs/examples/basic_consumers)  for basic consumer and consumers with different offsets.


### Server-side offset tracking

RabbitMQ Streams provides server-side offset tracking for consumers. This features allows a consuming application to restart consuming where it left off in a previous run.
You can use the store_offset (to store an offset in the server) and query_offset (to query it) methods of the consumer class like in this example:
- [server side offset tracking](https://github.com/qweeze/rstream/blob/master/docs/examples/manual_server_offset_tracking/consumer.py)


## Superstreams

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

### Managing disconnections:

The client does not support auto-reconnect at the moment.

When the TCP connection is disconnected unexpectedly, the client raises an event:

```python
def on_connection_closed(reason: Exception) -> None:
    print("connection has been closed for reason: " + str(reason))

consumer = Consumer(
..        
connection_closed_handler=on_connection_closed,
)
```

Please take a look at the complete example [here](https://github.com/qweeze/rstream/blob/master/docs/examples/check_connection_broken/consumer_handle_connections_issues.py)

## Load Balancer

In order to handle load balancers, you can use the `load_balancer_mode` parameter for producers and consumers. This will always attempt to create a connection via the load balancer, discarding connections that are inappropriate for the client type.

Producers must connect to the leader node, while consumers can connect to any, prioritizing replicas if available.


### Client Performances

The RabbitMQ Stream queues can handle high throughput. Currently, the client cannot reach the maximum throughput the server can handle. 

We found some bottlenecks; one of them is the current AMQP 1.0 marshal and unmarshal message format. 

This one:
```python
 for i in range(1_000_000):
            amqp_message = AMQPMessage(
                body="hello: {}".format(i),
            )
            # send is asynchronous
            await producer.send(stream=STREAM, message=amqp_message)
```

is more or less 50% slower than:
```python
 for i in range(1_000_000):
            # send is asynchronous
            await producer.send(stream=STREAM, message=b"hello")
```

You can use the `batch_send` to test the performances.

```python
$ python docs/examples/basic_producers/producer_send_batch_binary.py
Sent 1000000 messages in 6.7364 seconds. 148446.9526 messages per second
````

With AMQP 1.0 parser
```python
$ python docs/examples/basic_producers/producer_send_batch.py       
Sent 1000000 messages in 13.2724 seconds. 75344.4910 messages per second
```

We are evaluating to rewriting the `AMQP 1.0 codec` optimized for the stream use case.

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
