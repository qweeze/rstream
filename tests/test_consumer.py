# Copyright 2023 VMware, Inc. All Rights Reserved.
# SPDX-License-Identifier: MIT

import asyncio
import logging
import time
from functools import partial

import pytest
import uamqp

from rstream import (
    AMQPMessage,
    Consumer,
    ConsumerOffsetSpecification,
    FilterConfiguration,
    MessageContext,
    OffsetType,
    OnClosedErrorInfo,
    Producer,
    RouteType,
    SuperStreamConsumer,
    SuperStreamProducer,
    amqp_decoder,
    exceptions,
)

from .util import (
    consumer_update_handler_first,
    consumer_update_handler_next,
    consumer_update_handler_offset,
    on_message,
    routing_extractor_generic,
    run_consumer,
    task_to_delete_connection,
    task_to_delete_stream_consumer,
    wait_for,
)

pytestmark = pytest.mark.asyncio
logger = logging.getLogger(__name__)


async def test_create_stream_already_exists(stream: str, consumer: Consumer) -> None:
    with pytest.raises(exceptions.StreamAlreadyExists):
        await consumer.create_stream(stream)

    try:
        await consumer.create_stream(stream, exists_ok=True)
    except Exception:
        pytest.fail("Unexpected error")


async def test_delete_stream_doesnt_exist(consumer: Consumer) -> None:
    with pytest.raises(exceptions.StreamDoesNotExist):
        await consumer.delete_stream("not-existing-stream")

    try:
        await consumer.delete_stream("not-existing-stream", missing_ok=True)
    except Exception:
        pytest.fail("Unexpected error")


async def test_consume(stream: str, consumer: Consumer, producer: Producer) -> None:
    captured: list[bytes] = []
    await consumer.subscribe(
        stream, callback=lambda message, message_context: captured.append(bytes(message))
    )
    assert await producer.send_wait(stream, b"one") == 1
    assert await producer.send_batch(stream, [b"two", b"three"]) == [2, 3]

    await wait_for(lambda: len(captured) >= 3)
    assert captured == [b"one", b"two", b"three"]


async def test_offset_type_first(stream: str, consumer: Consumer, producer: Producer) -> None:
    captured: list[bytes] = []
    captured_offset: list[int] = []

    async def on_message_first(msg: AMQPMessage, message_context: MessageContext):
        captured_offset.append(message_context.offset)
        captured.append(bytes(msg))

    await consumer.subscribe(
        stream,
        callback=on_message_first,
        offset_specification=ConsumerOffsetSpecification(OffsetType.FIRST, None),
    )
    messages = [str(i).encode() for i in range(0, 10)]
    await producer.send_batch(stream, messages)

    await wait_for(lambda: len(captured) >= 10)
    assert captured == messages
    assert captured_offset == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]


async def test_offset_type_offset(stream: str, consumer: Consumer, producer: Producer) -> None:
    captured: list[bytes] = []
    captured_offset: list[int] = []

    async def on_message_offset(msg: AMQPMessage, message_context: MessageContext):
        captured_offset.append(message_context.offset)
        captured.append(bytes(msg))

    await consumer.subscribe(
        stream,
        callback=on_message_offset,
        offset_specification=ConsumerOffsetSpecification(OffsetType.OFFSET, 7),
    )
    messages = [str(i).encode() for i in range(0, 10)]
    await producer.send_batch(stream, messages)

    await wait_for(lambda: len(captured) >= 3)
    assert captured == messages[7:]
    assert captured_offset == [7, 8, 9]


async def test_offset_type_last(stream: str, consumer: Consumer, producer: Producer) -> None:
    messages = [str(i).encode() for i in range(1, 5_000)]
    await producer.send_batch(stream, messages)

    captured: list[bytes] = []
    await consumer.subscribe(
        stream,
        callback=lambda message, message_context: captured.append(bytes(message)),
        offset_specification=ConsumerOffsetSpecification(OffsetType.LAST, None),
        subscriber_name="test-subscriber",
    )

    await wait_for(lambda: len(captured) > 0 and captured[-1] == b"4999", 2)
    assert len(captured) < len(messages)


async def test_offset_manual_setting(stream: str, consumer: Consumer, producer: Producer) -> None:
    captured: list[bytes] = []
    await consumer.store_offset(stream=stream, offset=7, subscriber_name="test_offset_manual_setting")
    offset = await consumer.query_offset(stream=stream, subscriber_name="test_offset_manual_setting")

    assert offset == 7

    await consumer.subscribe(
        stream,
        callback=lambda message, message_context: captured.append(bytes(message)),
        offset_specification=ConsumerOffsetSpecification(OffsetType.OFFSET, offset),
    )

    messages = [str(i).encode() for i in range(1, 11)]
    await producer.send_batch(stream, messages)

    await wait_for(lambda: len(captured) >= 3)


async def test_consumer_callback(stream: str, consumer: Consumer, producer: Producer) -> None:
    streams: list[str] = []
    offsets: list[int] = []

    await consumer.subscribe(
        stream,
        callback=partial(
            on_message,
            streams=streams,
            offsets=offsets,
        ),
        offset_specification=ConsumerOffsetSpecification(OffsetType.FIRST, None),
    )

    messages = [str(i).encode() for i in range(0, 10)]
    await producer.send_batch(stream, messages)

    await wait_for(lambda: len(streams) >= 10)
    await wait_for(lambda: len(offsets) >= 10)

    for stream in streams:
        assert stream == "test-stream"

    for offset in offsets:
        assert offset >= 0 and offset < 100


async def test_offset_type_timestamp(stream: str, consumer: Consumer, producer: Producer) -> None:
    messages = [str(i).encode() for i in range(1, 5_000)]
    await producer.send_batch(stream, messages)

    # mark time in between message batches
    await asyncio.sleep(1)
    now = int(time.time() * 1000)

    messages = [str(i).encode() for i in range(5_000, 5_100)]
    await producer.send_batch(stream, messages)

    captured: list[bytes] = []

    await consumer.subscribe(
        stream,
        callback=lambda message, message_context: captured.append(bytes(message)),
        offset_specification=ConsumerOffsetSpecification(offset_type=OffsetType.TIMESTAMP, offset=now),
    )
    await wait_for(lambda: len(captured) > 0 and captured[0] >= b"5000", 2)


async def test_offset_type_next(stream: str, consumer: Consumer, producer: Producer) -> None:
    messages = [str(i).encode() for i in range(1, 11)]
    await producer.send_batch(stream, messages)

    captured: list[bytes] = []
    await consumer.subscribe(
        stream,
        callback=lambda message, message_context: captured.append(bytes(message)),
        offset_specification=ConsumerOffsetSpecification(OffsetType.NEXT, None),
        subscriber_name="test-subscriber",
    )
    await producer.send_wait(stream, b"11")
    await wait_for(lambda: len(captured) > 0)
    assert captured == [b"11"]


async def test_consume_with_resubscribe(stream: str, consumer: Consumer, producer: Producer) -> None:
    captured: list[bytes] = []
    subscriber_name = await consumer.subscribe(
        stream, callback=lambda message, message_context: captured.append(bytes(message))
    )
    await producer.send_wait(stream, b"one")
    await wait_for(lambda: len(captured) >= 1)

    await consumer.unsubscribe(subscriber_name)
    await consumer.subscribe(
        stream,
        callback=lambda message, message_context: captured.append(bytes(message)),
        offset_specification=ConsumerOffsetSpecification(OffsetType.NEXT, None),
    )

    await producer.send_wait(stream, b"two")
    await wait_for(lambda: len(captured) >= 2)
    assert captured == [b"one", b"two"]


async def test_consume_superstream_with_resubscribe(
    super_stream: str, super_stream_consumer: SuperStreamConsumer, super_stream_producer: SuperStreamProducer
) -> None:
    captured: list[bytes] = []
    await super_stream_consumer.subscribe(
        callback=lambda message, message_context: captured.append(bytes(message))
    )
    await super_stream_producer.send(b"one")
    await wait_for(lambda: len(captured) >= 1)

    await super_stream_consumer.unsubscribe()
    await super_stream_consumer.subscribe(
        callback=lambda message, message_context: captured.append(bytes(message)),
        offset_specification=ConsumerOffsetSpecification(OffsetType.NEXT, None),
    )

    await super_stream_producer.send(b"two")
    await wait_for(lambda: len(captured) >= 2)
    assert captured == [b"one", b"two"]


async def test_consume_with_restart(stream: str, consumer: Consumer, producer: Producer) -> None:
    captured: list[bytes] = []
    await consumer.subscribe(
        stream, callback=lambda message, message_context: captured.append(bytes(message))
    )
    await producer.send_wait(stream, b"one")
    await wait_for(lambda: len(captured) >= 1)

    await consumer.close()
    await consumer.start()
    await consumer.subscribe(
        stream,
        callback=lambda message, message_context: captured.append(bytes(message)),
        offset_specification=ConsumerOffsetSpecification(OffsetType.NEXT, None),
    )

    await producer.send_wait(stream, b"two")
    await wait_for(lambda: len(captured) >= 2)
    assert captured == [b"one", b"two"]


async def test_consume_multiple_streams(consumer: Consumer, producer: Producer) -> None:
    streams = ["stream1", "stream2", "stream3"]
    try:
        await asyncio.gather(*(consumer.create_stream(stream) for stream in streams))

        captured: list[bytes] = []
        await asyncio.gather(
            *(
                consumer.subscribe(
                    stream, callback=lambda message, message_context: captured.append(bytes(message))
                )
                for stream in streams
            )
        )

        await asyncio.gather(*(producer.send_wait(stream, b"test") for stream in streams))

        await wait_for(lambda: len(captured) >= 3)
        assert captured == [b"test", b"test", b"test"]

    finally:
        await producer.close()
        await asyncio.gather(*(consumer.delete_stream(stream) for stream in streams))


async def test_consume_superstream_with_sac_all_active(
    super_stream: str,
    super_stream_consumer_for_sac1: SuperStreamConsumer,
    super_stream_consumer_for_sac2: SuperStreamConsumer,
    super_stream_consumer_for_sac3: SuperStreamConsumer,
    super_stream_producer_for_sac: SuperStreamProducer,
) -> None:
    consumer_stream_list1: list[str] = []
    consumer_stream_list2: list[str] = []
    consumer_stream_list3: list[str] = []

    await run_consumer(super_stream_consumer_for_sac1, consumer_stream_list1)
    await run_consumer(super_stream_consumer_for_sac2, consumer_stream_list2)
    await run_consumer(super_stream_consumer_for_sac3, consumer_stream_list3)

    for i in range(10000):
        amqp_message = AMQPMessage(
            body="a:{}".format(i),
            properties=uamqp.message.MessageProperties(message_id=i),
        )
        await super_stream_producer_for_sac.send(amqp_message)

    await asyncio.sleep(1)

    # check that the total number of messages have been consumed by the consumers
    assert len(consumer_stream_list1) + len(consumer_stream_list2) + len(consumer_stream_list3) == 10000

    consumer_stream_set1 = set(consumer_stream_list1)
    consumer_stream_set2 = set(consumer_stream_list2)
    consumer_stream_set3 = set(consumer_stream_list3)

    # check that every consumer consumed in just one set
    assert len(consumer_stream_set1) == 1
    assert len(consumer_stream_set2) == 1
    assert len(consumer_stream_set3) == 1

    consumers_set = consumer_stream_set1.union(consumer_stream_set2)
    consumers_set = consumers_set.union(consumer_stream_list3)

    assert len(consumers_set) == 3


async def test_consume_superstream_with_sac_one_non_active(
    super_stream: str,
    super_stream_consumer_for_sac1: SuperStreamConsumer,
    super_stream_consumer_for_sac2: SuperStreamConsumer,
    super_stream_consumer_for_sac3: SuperStreamConsumer,
    super_stream_consumer_for_sac4: SuperStreamConsumer,
    super_stream_producer_for_sac: SuperStreamProducer,
) -> None:
    consumer_stream_list1: list[str] = []
    consumer_stream_list2: list[str] = []
    consumer_stream_list3: list[str] = []
    consumer_stream_list4: list[str] = []

    await run_consumer(super_stream_consumer_for_sac1, consumer_stream_list1)
    await run_consumer(super_stream_consumer_for_sac2, consumer_stream_list2)
    await run_consumer(super_stream_consumer_for_sac3, consumer_stream_list3)
    await run_consumer(super_stream_consumer_for_sac4, consumer_stream_list4)

    for i in range(10000):
        amqp_message = AMQPMessage(
            body="a:{}".format(i),
            properties=uamqp.message.MessageProperties(message_id=i),
        )
        await super_stream_producer_for_sac.send(amqp_message)

    await asyncio.sleep(1)

    # check that the total number of messages have been consumed by the consumers
    assert len(consumer_stream_list1) + len(consumer_stream_list2) + len(consumer_stream_list3) == 10000

    consumer_stream_set1 = set(consumer_stream_list1)
    consumer_stream_set2 = set(consumer_stream_list2)
    consumer_stream_set3 = set(consumer_stream_list3)
    consumer_stream_set4 = set(consumer_stream_list4)

    assert (
        len(consumer_stream_set1) == 0
        or len(consumer_stream_set2) == 0
        or len(consumer_stream_set3) == 0
        or len(consumer_stream_set4) == 0
    )

    consumers_set = consumer_stream_set1.union(consumer_stream_set2)
    consumers_set = consumers_set.union(consumer_stream_list3)
    consumers_set = consumers_set.union(consumer_stream_list4)

    # one consumer was alway inactive
    assert len(consumers_set) == 3


async def test_consume_superstream_with_callback_next(
    super_stream: str,
    super_stream_consumer_for_sac1: SuperStreamConsumer,
    super_stream_consumer_for_sac2: SuperStreamConsumer,
    super_stream_consumer_for_sac3: SuperStreamConsumer,
    super_stream_producer_for_sac: SuperStreamProducer,
) -> None:
    consumer_stream_list1: list[str] = []
    consumer_stream_list2: list[str] = []
    consumer_stream_list3: list[str] = []

    await run_consumer(super_stream_consumer_for_sac1, consumer_stream_list1, consumer_update_handler_next)
    await run_consumer(super_stream_consumer_for_sac2, consumer_stream_list2, consumer_update_handler_next)
    await run_consumer(super_stream_consumer_for_sac3, consumer_stream_list3, consumer_update_handler_next)

    for i in range(10000):
        amqp_message = AMQPMessage(
            body="a:{}".format(i),
            properties=uamqp.message.MessageProperties(message_id=i),
        )
        await super_stream_producer_for_sac.send(amqp_message)

    await asyncio.sleep(1)

    # check that the total number of messages have been consumed by the consumers
    assert len(consumer_stream_list1) + len(consumer_stream_list2) + len(consumer_stream_list3) == 10000

    consumer_stream_set1 = set(consumer_stream_list1)
    consumer_stream_set2 = set(consumer_stream_list2)
    consumer_stream_set3 = set(consumer_stream_list3)

    # check that every consumer consumed in just one set
    assert len(consumer_stream_set1) == 1
    assert len(consumer_stream_set2) == 1
    assert len(consumer_stream_set3) == 1

    consumers_set = consumer_stream_set1.union(consumer_stream_set2)
    consumers_set = consumers_set.union(consumer_stream_list3)

    assert len(consumers_set) == 3


async def test_consume_superstream_with_callback_first(
    super_stream: str,
    super_stream_consumer_for_sac1: SuperStreamConsumer,
    super_stream_consumer_for_sac2: SuperStreamConsumer,
    super_stream_consumer_for_sac3: SuperStreamConsumer,
    super_stream_producer_for_sac: SuperStreamProducer,
) -> None:
    consumer_stream_list1: list[str] = []
    consumer_stream_list2: list[str] = []
    consumer_stream_list3: list[str] = []

    await run_consumer(super_stream_consumer_for_sac1, consumer_stream_list1, consumer_update_handler_first)
    await run_consumer(super_stream_consumer_for_sac2, consumer_stream_list2, consumer_update_handler_first)
    await run_consumer(super_stream_consumer_for_sac3, consumer_stream_list3, consumer_update_handler_first)

    for i in range(10000):
        amqp_message = AMQPMessage(
            body="a:{}".format(i),
            properties=uamqp.message.MessageProperties(message_id=i),
        )
        await super_stream_producer_for_sac.send(amqp_message)

    await asyncio.sleep(1)

    # check that the total number of messages have been consumed by the consumers
    assert len(consumer_stream_list1) + len(consumer_stream_list2) + len(consumer_stream_list3) == 10000

    consumer_stream_set1 = set(consumer_stream_list1)
    consumer_stream_set2 = set(consumer_stream_list2)
    consumer_stream_set3 = set(consumer_stream_list3)

    # check that every consumer consumed in just one set
    assert len(consumer_stream_set1) == 1
    assert len(consumer_stream_set2) == 1
    assert len(consumer_stream_set3) == 1

    consumers_set = consumer_stream_set1.union(consumer_stream_set2)
    consumers_set = consumers_set.union(consumer_stream_list3)

    assert len(consumers_set) == 3


async def test_consume_superstream_with_callback_offset(
    super_stream: str,
    super_stream_consumer_for_sac1: SuperStreamConsumer,
    super_stream_consumer_for_sac2: SuperStreamConsumer,
    super_stream_consumer_for_sac3: SuperStreamConsumer,
    super_stream_producer_for_sac: SuperStreamProducer,
) -> None:
    consumer_stream_list1: list[str] = []
    consumer_stream_list2: list[str] = []
    consumer_stream_list3: list[str] = []

    await run_consumer(super_stream_consumer_for_sac1, consumer_stream_list1, consumer_update_handler_offset)
    await run_consumer(super_stream_consumer_for_sac2, consumer_stream_list2, consumer_update_handler_offset)
    await run_consumer(super_stream_consumer_for_sac3, consumer_stream_list3, consumer_update_handler_offset)

    for i in range(10000):
        amqp_message = AMQPMessage(
            body="a:{}".format(i),
            properties=uamqp.message.MessageProperties(message_id=i),
        )
        await super_stream_producer_for_sac.send(amqp_message)

    await asyncio.sleep(1)

    # check that the total number of messages have been consumed by the consumers
    assert len(consumer_stream_list1) + len(consumer_stream_list2) + len(consumer_stream_list3) == 10000

    consumer_stream_set1 = set(consumer_stream_list1)
    consumer_stream_set2 = set(consumer_stream_list2)
    consumer_stream_set3 = set(consumer_stream_list3)

    # check that every consumer consumed in just one set
    assert len(consumer_stream_set1) == 1
    assert len(consumer_stream_set2) == 1
    assert len(consumer_stream_set3) == 1

    consumers_set = consumer_stream_set1.union(consumer_stream_set2)
    consumers_set = consumers_set.union(consumer_stream_list3)

    assert len(consumers_set) == 3


async def test_callback_sync_request(stream: str, consumer: Consumer, producer: Producer) -> None:
    captured: list[bytes] = []

    async def on_message_first(msg: AMQPMessage, message_context: MessageContext):
        captured.append(bytes(msg))
        await consumer.close()

    await consumer.subscribe(stream, callback=on_message_first)
    messages = [str(i).encode() for i in range(0, 1)]
    await producer.send_batch(stream, messages)

    await wait_for(lambda: len(captured) >= 1)


async def test_consumer_connection_broke(stream: str) -> None:

    connection_broke = False
    stream_disconnected = None
    consumer_broke: Consumer

    async def on_connection_closed(disconnection_info: OnClosedErrorInfo) -> None:
        nonlocal connection_broke
        connection_broke = True
        nonlocal consumer_broke
        nonlocal stream_disconnected
        stream_disconnected = disconnection_info.streams.pop()

        await consumer_broke.close()

    consumer_broke = Consumer(
        host="localhost",
        port=5552,
        vhost="/",
        username="guest",
        password="guest",
        on_close_handler=on_connection_closed,
        connection_name="test-connection",
    )

    async def on_message(msg: AMQPMessage, message_context: MessageContext):
        message_context.consumer.get_stream(message_context.subscriber_name)

    asyncio.create_task(task_to_delete_connection("test-connection"))

    await consumer_broke.start()
    await consumer_broke.subscribe(stream=stream, callback=on_message, decoder=amqp_decoder)
    await consumer_broke.run()

    assert connection_broke is True
    assert stream_disconnected == stream

    await asyncio.sleep(1)


async def test_super_stream_consumer_connection_broke(super_stream: str) -> None:

    connection_broke = False
    streams_disconnected: set[str] = set()
    consumer_broke: Consumer

    async def on_connection_closed(disconnection_info: OnClosedErrorInfo) -> None:
        nonlocal connection_broke
        nonlocal streams_disconnected
        # avoiding multiple connection closed to hit
        if connection_broke is True:
            for stream in disconnection_info.streams:
                streams_disconnected.add(stream)
            return None

        connection_broke = True

        for stream in disconnection_info.streams:
            streams_disconnected.add(stream)

        await super_stream_consumer_broke.close()

    super_stream_consumer_broke = SuperStreamConsumer(
        host="localhost",
        port=5552,
        vhost="/",
        username="guest",
        password="guest",
        on_close_handler=on_connection_closed,
        connection_name="test-connection",
        super_stream=super_stream,
    )

    async def on_message(msg: AMQPMessage, message_context: MessageContext):
        message_context.consumer.get_stream(message_context.subscriber_name)

    asyncio.create_task(task_to_delete_connection("test-connection"))

    await super_stream_consumer_broke.start()
    await super_stream_consumer_broke.subscribe(callback=on_message, decoder=amqp_decoder)
    await super_stream_consumer_broke.run()

    assert connection_broke is True
    assert "test-super-stream-0" in streams_disconnected
    assert "test-super-stream-1" in streams_disconnected
    assert "test-super-stream-2" in streams_disconnected


# Send a few messages to a superstream, consume, simulate a disconnection and check for reconnection
# from offset 0
async def test_super_stream_consumer_connection_broke_with_reconnect(super_stream: str) -> None:

    connection_broke = False
    streams_disconnected: set[str] = set()
    consumer_broke: Consumer
    offset_restart = 0

    async def on_connection_closed(disconnection_info: OnClosedErrorInfo) -> None:
        logger.warning("connection closed")
        nonlocal connection_broke
        nonlocal streams_disconnected
        # avoiding multiple connection closed to hit
        if connection_broke is True:
            for stream in disconnection_info.streams:
                logger.warning("reconnecting")
                streams_disconnected.add(stream)
            return None

        connection_broke = True

        for stream in disconnection_info.streams:
            streams_disconnected.add(stream)
            # start from stored offset
            await super_stream_consumer_broke.reconnect_stream(stream, offset_restart)

    # Sending a few messages in the stream in order to be consumed
    super_stream_producer_broke = SuperStreamProducer(
        "localhost",
        username="guest",
        password="guest",
        routing_extractor=routing_extractor_generic,
        routing=RouteType.Hash,
        connection_name="test-connection",
        super_stream=super_stream,
    )

    await super_stream_producer_broke.start()

    i = 0
    for i in range(0, 10000):
        amqp_message = AMQPMessage(
            body="hello: {}".format(i),
            application_properties={"id": "{}".format(i)},
        )
        await super_stream_producer_broke.send(message=amqp_message)

    await super_stream_producer_broke.close()

    super_stream_consumer_broke = SuperStreamConsumer(
        host="localhost",
        port=5552,
        vhost="/",
        username="guest",
        password="guest",
        on_close_handler=on_connection_closed,
        connection_name="test-connection",
        super_stream=super_stream,
    )

    async def on_message(msg: AMQPMessage, message_context: MessageContext):
        nonlocal connection_broke
        message_context.consumer.get_stream(message_context.subscriber_name)
        # check after disconnection offset have been reset
        if connection_broke is True:
            assert message_context.offset == offset_restart
            await super_stream_consumer_broke.close()

    asyncio.create_task(task_to_delete_connection("test-connection"))

    await super_stream_consumer_broke.start()
    await super_stream_consumer_broke.subscribe(callback=on_message, decoder=amqp_decoder)
    await super_stream_consumer_broke.run()

    assert connection_broke is True
    assert "test-super-stream-0" in streams_disconnected
    assert "test-super-stream-1" in streams_disconnected
    assert "test-super-stream-2" in streams_disconnected


async def test_consume_filtering(stream: str, consumer: Consumer, producer_with_filtering: Producer) -> None:

    filters = ["1"]

    captured: list[bytes] = []

    async def on_message(msg: AMQPMessage, message_context: MessageContext):
        captured.append(bytes(msg))

    await consumer.subscribe(
        stream,
        callback=on_message,
        decoder=amqp_decoder,
        filter_input=FilterConfiguration(
            values_to_filter=filters,
            predicate=lambda message: message.application_properties[b"id"] == filters[0].encode("utf-8"),
            match_unfiltered=False,
        ),
    )

    for j in range(10):

        messages = []
        for i in range(50):
            application_properties = {
                "id": str(i),
            }
            amqp_message = AMQPMessage(
                body="hello: {}".format(i),
                application_properties=application_properties,
            )
            messages.append(amqp_message)
        # send_batch is synchronous. will wait till termination
        await producer_with_filtering.send_batch(stream=stream, batch=messages)  # type: ignore

    # Consumed just the filetered items
    await wait_for(lambda: len(captured) == 10)


async def test_consume_filtering_match_unfiltered(
    stream: str, consumer: Consumer, producer: Producer
) -> None:

    filters = ["1"]

    captured: list[bytes] = []

    async def on_message(msg: AMQPMessage, message_context: MessageContext):
        captured.append(bytes(msg))

    await consumer.subscribe(
        stream,
        callback=on_message,
        decoder=amqp_decoder,
        filter_input=FilterConfiguration(
            values_to_filter=filters,
            predicate=lambda message: message.application_properties[b"id"] == filters[0].encode("utf-8"),
            match_unfiltered=False,
        ),
    )

    for j in range(10):

        messages = []
        for i in range(50):
            application_properties = {
                "id": str(i),
            }
            amqp_message = AMQPMessage(
                body="hello: {}".format(i),
                application_properties=application_properties,
            )
            messages.append(amqp_message)
        # send_batch is synchronous. will wait till termination
        await producer.send_batch(stream=stream, batch=messages)  # type: ignore

    # No filter on produce side no filetering
    await wait_for(lambda: len(captured) == 0)


async def test_consumer_metadata_update(consumer: Consumer) -> None:

    consumer_closed = False
    stream_disconnected = None
    stream = "test-stream-metadata-update"
    consumer_metadata_update: Consumer

    async def on_connection_closed(disconnection_info: OnClosedErrorInfo) -> None:
        nonlocal consumer_closed

        nonlocal consumer_metadata_update
        nonlocal stream_disconnected
        stream_disconnected = disconnection_info.streams.pop()

        if consumer_closed is False:
            consumer_closed = True
            await consumer_metadata_update.close()

    consumer_metadata_update = Consumer(
        host="localhost",
        port=5552,
        vhost="/",
        username="guest",
        password="guest",
        on_close_handler=on_connection_closed,
        connection_name="test-connection",
    )

    async def on_message(msg: AMQPMessage, message_context: MessageContext):
        message_context.consumer.get_stream(message_context.subscriber_name)

    await consumer_metadata_update.start()
    await consumer_metadata_update.create_stream(stream)
    asyncio.create_task(task_to_delete_stream_consumer(consumer, stream))
    await consumer_metadata_update.subscribe(stream=stream, callback=on_message, decoder=amqp_decoder)
    await consumer_metadata_update.run()

    assert consumer_closed is True
    assert stream_disconnected == stream

    await asyncio.sleep(1)
