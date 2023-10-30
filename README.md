# RabbitMQ Stream Python Client

A Python asyncio-based client for [RabbitMQ Streams](https://www.rabbitmq.com/stream.html)


The RabbitMQ stream plug-in is required. See the [documentation](https://www.rabbitmq.com/stream.html#enabling-plugin) for enabling it.


# Table of Contents


- [Installation](#installation)
- [Examples](#examples)
- [Client Codecs](#client-codecs)
    * [AMQP 1.0 codec  vs Binary](#amqp-10-codec--vs-binary)
- [Publishing messages](#publishing-messages)
    * [Publishing with confirmation](#publishing-with-confirmation)
- [Sub-Entry Batching and Compression](#sub-entry-batching-and-compression)
- [Deduplication](#deduplication)
- [Consuming messages](#consuming-messages)
    * [Server-side offset tracking](#server-side-offset-tracking)
- [Superstreams](#superstreams)
- [Single Active Consumer](#single-active-consumer)
- [Connecting with SSL](#connecting-with-ssl)
- [Managing disconnections](#managing-disconnections)
- [Load Balancer](#load-balancer)
- [Client Performances](#client-performances)
- [Build and Test](#build-and-test)
- [Project Notes](#project-notes)


## Installation

The RabbitMQ stream plug-in is required. See the [documentation](https://www.rabbitmq.com/stream.html#enabling-plugin) for enabling it.


The client is distributed via [`PIP`](https://pypi.org/project/rstream/):
```bash
 pip install rstream
```

## Examples

[Here](https://github.com/qweeze/rstream/blob/master/docs/examples/) you can find different examples.


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
* `send_sub_entry`: asynchronous. See [Sub-entry batching and compression](#sub-entry-batching-and-compression).

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


## Sub-Entry Batching and Compression
RabbitMQ Stream provides a special mode to publish, store, and dispatch messages: sub-entry batching. This mode increases throughput at the cost of increased latency and potential duplicated messages even when deduplication is enabled. It also allows using compression to reduce bandwidth and storage if messages are reasonably similar, at the cost of increasing CPU usage on the client side.

Sub-entry batching consists in squeezing several messages – a batch – in the slot that is usually used for one message. This means outbound messages are not only batched in publishing frames, but in sub-entries as well.

```python

  # sending with compression
   await producer.send_sub_entry(
        STREAM, compression_type=CompressionType.Gzip, sub_entry_messages=messages
   )
```
[Full example producer using sub-entry batch](https://github.com/qweeze/rstream/blob/master/docs/examples/sub_entry_batch/producer_sub_entry_batch.py)

Consumer side is automatic, so no need configurations. 

The client is shipped with No Compression (`CompressionType.No`) and Gzip Compression (`CompressionType.Gzip`) the other compressions (`Snappy`, `Lz4`, `Zstd`) can be used implementing the `ICompressionCodec` class. 



## Deduplication

RabbitMQ Stream can detect and filter out duplicated messages, based on 2 client-side elements: the producer name and the message publishing ID.
All the producer methods to send messages (send, send_batch, send_wait) takes a publisher_name parameter while the message publishing id can be set in the AMQP message.

Example:
- [producer with deduplication](https://github.com/qweeze/rstream/blob/master/docs/examples/deduplication/producer_ded.py)

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


See the [Super Stream example](https://github.com/qweeze/rstream/tree/master/docs/examples/super_stream)

### Single Active Consumer

Single active consumer provides exclusive consumption and consumption continuity on a stream. <br /> 
See the [blog post](https://blog.rabbitmq.com/posts/2022/07/rabbitmq-3-11-feature-preview-single-active-consumer-for-streams) for more info.
See examples in: 

See the [single active consumer example](https://github.com/qweeze/rstream/blob/master/docs/examples/single_active_consumer/)

### Connecting with SSL

You can enable ssl/tls.
See example here:
[tls example](https://github.com/qweeze/rstream/blob/master/docs/examples/tls/producer.py)

### Managing disconnections

The client does not support auto-reconnect at the moment.

When the TCP connection is disconnected unexpectedly, the client raises an event:

```python
async def on_connection_closed(disconnection_info: DisconnectionErrorInfo) -> None:
    print(
        "connection has been closed from stream: "
        + str(disconnection_info.streams)
        + " for reason: "
        + str(disconnection_info.reason)
    )

consumer = Consumer(
..        
connection_closed_handler=on_connection_closed,
)
```

Please take a look at the complete examples [here](https://github.com/qweeze/rstream/blob/master/docs/examples/check_connection_broken/)

## Load Balancer

In order to handle load balancers, you can use the `load_balancer_mode` parameter for producers and consumers. This will always attempt to create a connection via the load balancer, discarding connections that are inappropriate for the client type.

Producers must connect to the leader node, while consumers can connect to any, prioritizing replicas if available.


## Client Performances

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


## Build and Test

To run the tests, you need to have a running RabbitMQ Stream server. 
You can use the docker official image.

Run the server with the following command:
```bash
docker run -it --rm --name rabbitmq -p 5552:5552 -p 5672:5672 -p 15672:15672 \
    -e RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS='-rabbitmq_stream advertised_host localhost' \
    rabbitmq:3.12-management
```

enable the plugin:
```bash
docker exec rabbitmq rabbitmq-plugins enable rabbitmq_stream
```

and run the tests:
```bash
 poetry run pytest
```


## Project Notes
The project is in development and stabilization phase. Features and API are subject to change, but breaking changes will be kept to a minimum. </br>
Any feedback or contribution is welcome
