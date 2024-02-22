import asyncio
import json
import signal
import time

# Set of import from rsteram needed for the various functionalities
from rstream import (
    AMQPMessage,
    ConfirmationStatus,
    Consumer,
    ConsumerOffsetSpecification,
    MessageContext,
    OffsetType,
    OnClosedErrorInfo,
    Producer,
    RouteType,
    SuperStreamConsumer,
    SuperStreamProducer,
    amqp_decoder,
)

# global variables needed by the test
confirmed_count = 0
messages_consumed = 0
messages_per_producer = 0
producer: Producer | SuperStreamProducer
consumer: Consumer | SuperStreamConsumer

# Load configuration file (appsettings.json)
async def load_json_file(configuration_file: str) -> dict:
    data = open("./python_rstream/appsettings.json")
    return json.load(data)


# Routing instruction for SuperStream Producer
async def routing_extractor(message: AMQPMessage) -> str:
    return message.application_properties["id"]


# Make producers (producer or superstream producer)
async def make_producer(rabbitmq_data: dict) -> Producer | SuperStreamProducer:
    host = rabbitmq_data["Host"]
    username = rabbitmq_data["Username"]
    password = rabbitmq_data["Password"]
    port = rabbitmq_data["Port"]
    vhost = rabbitmq_data["Virtualhost"]
    load_balancer = bool(rabbitmq_data["LoadBalancer"])
    stream_name = rabbitmq_data["StreamName"]

    producer: Producer | SuperStreamProducer

    if bool(rabbitmq_data["SuperStream"]) is False:

        producer = Producer(
            host=host,
            username=username,
            password=password,
            port=port,
            vhost=vhost,
            load_balancer_mode=load_balancer,
        )

    else:

        producer = SuperStreamProducer(
            host=host,
            username=username,
            password=password,
            port=port,
            vhost=vhost,
            load_balancer_mode=load_balancer,
            super_stream=stream_name,
            routing=RouteType.Hash,
            routing_extractor=routing_extractor,
        )

    return producer


# metadata and disconnection events for consumers
async def on_close_connection(on_closed_info: OnClosedErrorInfo) -> None:

    print(
        "connection has been closed from stream: "
        + str(on_closed_info.streams)
        + " for reason: "
        + str(on_closed_info.reason)
    )

    await asyncio.sleep(2)
    # reconnect just if the partition exists
    for stream in on_closed_info.streams:
        backoff = 1
        while True:
            try:
                print("reconnecting stream: {}".format(stream))
                await consumer.reconnect_stream(stream)
                break
            except Exception as ex:
                if backoff > 32:
                    # failed to found the leader
                    print("reconnection failed")
                    break
                backoff = backoff * 2
                print("exception reconnecting waiting 120s: " + str(ex))
                await asyncio.sleep(30)
                continue


# Make consumers
async def make_consumer(rabbitmq_data: dict) -> Consumer | SuperStreamConsumer:

    host = rabbitmq_data["Host"]
    username = rabbitmq_data["Username"]
    password = rabbitmq_data["Password"]
    port = rabbitmq_data["Port"]
    vhost = rabbitmq_data["Virtualhost"]
    load_balancer = bool(rabbitmq_data["LoadBalancer"])
    stream_name = rabbitmq_data["StreamName"]

    consumer: Consumer | SuperStreamConsumer

    if bool(rabbitmq_data["SuperStream"]) is False:

        consumer = Consumer(
            host=host,
            username=username,
            password=password,
            port=port,
            vhost=vhost,
            load_balancer_mode=load_balancer,
            on_close_handler=on_close_connection,
        )

    else:

        consumer = SuperStreamConsumer(
            host=host,
            username=username,
            password=password,
            port=port,
            vhost=vhost,
            load_balancer_mode=load_balancer,
            super_stream=stream_name,
            on_close_handler=on_close_connection,
        )

    return consumer


# Where the confirmation happens
async def _on_publish_confirm_client(confirmation: ConfirmationStatus) -> None:
    global confirmed_count
    if confirmation.is_confirmed:
        confirmed_count = confirmed_count + 1
    else:
        print(
            "message id: {} not confirmed. Response code {}".format(
                confirmation.message_id, confirmation.response_code
            )
        )


async def on_message(msg: AMQPMessage, message_context: MessageContext):
    global messages_consumed
    messages_consumed = messages_consumed + 1
    # some printf after some messages consumed in order to check that we are working...
    if (messages_consumed % 100000) == 0:
        stream = await message_context.consumer.stream(message_context.subscriber_name)
        offset = message_context.offset
        print("Received message: {} from stream: {} - message offset: {}".format(msg, stream, offset))

    if messages_consumed == (messages_per_producer * 3):
        print("CONSUMED ALL MESSAGES PUBLISHED...")


async def publish(rabbitmq_configuration: dict):

    global producer
    global messages_per_producer

    stream_name = rabbitmq_configuration["StreamName"]
    is_super_stream_scenario = bool(rabbitmq_configuration["SuperStream"])
    messages_per_producer = int(rabbitmq_configuration["MessagesPerProducer"])
    producers = int(rabbitmq_configuration["Producers"])
    delay_sending_msg = int(rabbitmq_configuration["DelayDuringSendMs"])

    producer = await make_producer(rabbitmq_configuration)
    await producer.start()

    # create a stream if it doesn't already exist
    if not is_super_stream_scenario:
        for p in range(producers):
            await producer.create_stream(stream_name + "-" + str(p), exists_ok=True)  # type: ignore

    start_time = time.perf_counter()

    for i in range(messages_per_producer):
        try:
            await asyncio.sleep(delay_sending_msg)
        except asyncio.exceptions.CancelledError:
            print("exception in sleeping")
            return

        amqp_message = AMQPMessage(
            body="hello: {}".format(i),
            application_properties={"id": "{}".format(i)},
        )
        # send is asynchronous
        if not is_super_stream_scenario:
            for p in range(producers):
                try:
                    await producer.send(
                        stream=stream_name + "-" + str(p),
                        message=amqp_message,
                        on_publish_confirm=_on_publish_confirm_client,
                    )  # type: ignore
                except Exception as ex:
                    print("Exception in send: " + str(ex))

        else:
            try:
                await producer.send(message=amqp_message, on_publish_confirm=_on_publish_confirm_client)  # type: ignore
            except Exception as ex:
                print("Exception in send: " + str(ex))

    await producer.close()

    end_time = time.perf_counter()
    print(
        f"Sent {messages_per_producer} messages for each of the {producers} producers in {end_time - start_time:0.4f} seconds"
    )

    # the number of confirmed messages should be the same as the total messages we sent
    print("confirmed_count: " + str(confirmed_count))
    print("messages_per_producer: " + str(messages_per_producer))
    assert confirmed_count == (messages_per_producer * 3)


async def consume(rabbitmq_configuration: dict):

    global consumer

    is_super_stream_scenario = bool(rabbitmq_configuration["SuperStream"])
    consumers = int(rabbitmq_configuration["Consumers"])
    stream_name = rabbitmq_configuration["StreamName"]

    consumer = await make_consumer(rabbitmq_configuration)

    # create a stream if it doesn't already exist
    if not is_super_stream_scenario:
        for p in range(consumers):
            await consumer.create_stream(stream_name + "-" + str(p), exists_ok=True)  # type: ignore

    offset_spec = ConsumerOffsetSpecification(OffsetType.LAST, None)
    await consumer.start()
    if not is_super_stream_scenario:
        for c in range(consumers):
            await consumer.subscribe(
                stream=stream_name + "-" + str(c),
                callback=on_message,
                decoder=amqp_decoder,
                offset_specification=offset_spec,
            )  # type: ignore
    else:
        await consumer.subscribe(callback=on_message, decoder=amqp_decoder, offset_specification=offset_spec)  # type: ignore

    await consumer.run()


async def close(producer_task: asyncio.Task, consumer_task: asyncio.Task):

    global producer
    global consumer

    await producer.close()
    await consumer.close()

    producer_task.cancel()
    consumer_task.cancel()


async def main():

    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGINT, lambda: asyncio.create_task(close(producer_task, consumer_task)))

    configuration = await load_json_file("appsettings.json")
    rabbitmq_configuration = configuration["RabbitMQ"]

    producer_task = asyncio.create_task(publish(rabbitmq_configuration))
    consumer_task = asyncio.create_task(consume(rabbitmq_configuration))

    await producer_task
    await consumer_task


asyncio.run(main())
