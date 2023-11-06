# Copyright 2023 VMware, Inc. All Rights Reserved.
# SPDX-License-Identifier: MIT

import asyncio
import logging
from functools import partial

import pytest

from rstream import (
    AMQPMessage,
    CompressionType,
    Consumer,
    DisconnectionErrorInfo,
    Producer,
    RawMessage,
    RouteType,
    SuperStreamConsumer,
    SuperStreamProducer,
    amqp_decoder,
    exceptions,
)

from .util import (
    on_publish_confirm_client_callback,
    on_publish_confirm_client_callback2,
    routing_extractor_generic,
    task_to_delete_connection,
    wait_for,
)

logger = logging.getLogger(__name__)
pytestmark = pytest.mark.asyncio


async def test_create_stream_already_exists(stream: str, producer: Producer) -> None:
    with pytest.raises(exceptions.StreamAlreadyExists):
        await producer.create_stream(stream)

    try:
        await producer.create_stream(stream, exists_ok=True)
    except Exception:
        pytest.fail("Unexpected error")


async def test_delete_stream_doesnt_exist(producer: Producer) -> None:
    with pytest.raises(exceptions.StreamDoesNotExist):
        await producer.delete_stream("not-existing-stream")

    try:
        await producer.delete_stream("not-existing-stream", missing_ok=True)
    except Exception:
        pytest.fail("Unexpected error")


async def test_publishing_sequence(stream: str, producer: Producer, consumer: Consumer) -> None:
    captured: list[bytes] = []
    await consumer.subscribe(
        stream, callback=lambda message, message_context: captured.append(bytes(message))
    )

    assert await producer.send_wait(stream, b"one") == 1
    assert await producer.send_batch(stream, [b"two", b"three"]) == [2, 3]
    await wait_for(lambda: len(captured) == 3)
    assert captured == [b"one", b"two", b"three"]


async def test_publishing_several_messages(stream: str, producer: Producer, consumer: Consumer) -> None:
    captured: list[bytes] = []
    await consumer.subscribe(
        stream, callback=lambda message, message_context: captured.append(bytes(message))
    )

    for i in range(0, 100000):
        await producer.send(stream, b"one")

    await wait_for(lambda: len(captured) == 100000)


async def test_publishing_sequence_subbatching_nocompression(
    stream: str, producer: Producer, consumer: Consumer
) -> None:
    captured: list[bytes] = []

    await consumer.subscribe(
        stream, callback=lambda message, message_context: captured.append(bytes(message))
    )

    list_messages = []
    list_messages.append(b"one")
    list_messages.append(b"two")
    list_messages.append(b"three")

    await producer.send_sub_entry(stream, list_messages, compression_type=CompressionType.No)

    await wait_for(lambda: len(captured) == 3)
    assert captured == [b"one", b"two", b"three"]


async def test_publishing_sequence_subbatching_gzip(
    stream: str, producer: Producer, consumer: Consumer
) -> None:
    captured: list[bytes] = []

    await consumer.subscribe(
        stream, callback=lambda message, message_context: captured.append(bytes(message))
    )

    list_messages = []
    list_messages.append(b"one")
    list_messages.append(b"two")
    list_messages.append(b"three")

    await producer.send_sub_entry(stream, list_messages, compression_type=CompressionType.Gzip)

    await wait_for(lambda: len(captured) == 3)
    assert captured == [b"one", b"two", b"three"]


async def test_publishing_sequence_subbatching_mix(
    stream: str, producer: Producer, consumer: Consumer
) -> None:
    captured: list[bytes] = []

    await consumer.subscribe(
        stream, callback=lambda message, message_context: captured.append(bytes(message))
    )

    list_messages = []
    list_messages.append(b"one")
    list_messages.append(b"two")
    list_messages.append(b"three")

    await producer.send_batch(stream, list_messages)
    await producer.send_sub_entry(stream, list_messages, compression_type=CompressionType.Gzip)
    await producer.send_sub_entry(stream, list_messages, compression_type=CompressionType.No)
    await producer.send_sub_entry(stream, list_messages, compression_type=CompressionType.Gzip)

    await wait_for(lambda: len(captured) == 12)
    assert captured == [
        b"one",
        b"two",
        b"three",
        b"one",
        b"two",
        b"three",
        b"one",
        b"two",
        b"three",
        b"one",
        b"two",
        b"three",
    ]


async def test_publishing_sequence_async(stream: str, producer: Producer, consumer: Consumer) -> None:
    captured: list[bytes] = []

    await consumer.subscribe(
        stream, callback=lambda message, message_context: captured.append(bytes(message))
    )

    await producer.send(stream, b"one")
    await producer.send(stream, b"two")
    await producer.send(stream, b"three")

    await wait_for(lambda: len(captured) == 3)
    assert captured == [b"one", b"two", b"three"]


async def test_publish_deduplication(stream: str, producer: Producer, consumer: Consumer) -> None:
    captured: list[bytes] = []
    await consumer.subscribe(
        stream, callback=lambda message, message_context: captured.append(bytes(message))
    )

    async def publish_with_ids(*ids):
        for publishing_id in ids:
            await producer.send_wait(
                stream,
                RawMessage(f"test_{publishing_id}".encode(), publishing_id),
            )

    await publish_with_ids(1, 2, 3)
    await publish_with_ids(1, 2, 3)

    await wait_for(lambda: len(captured) == 3)
    assert captured == [b"test_1", b"test_2", b"test_3"]

    await publish_with_ids(2, 3, 4)

    await wait_for(lambda: len(captured) == 4)
    assert captured == [b"test_1", b"test_2", b"test_3", b"test_4"]


async def test_publish_deduplication_async(stream: str, producer: Producer, consumer: Consumer) -> None:
    captured: list[bytes] = []
    await consumer.subscribe(
        stream, callback=lambda message, message_context: captured.append(bytes(message))
    )

    async def publish_with_ids(*ids):
        for publishing_id in ids:
            await producer.send(
                stream,
                RawMessage(f"test_{publishing_id}".encode(), publishing_id),
            )

    await publish_with_ids(1, 2, 3)
    await publish_with_ids(1, 2, 3)

    # give some time to the background thread to publish
    await asyncio.sleep(1)
    await wait_for(lambda: len(captured) == 3)
    assert captured == [b"test_1", b"test_2", b"test_3"]

    await asyncio.sleep(1)
    await publish_with_ids(2, 3, 4)

    await wait_for(lambda: len(captured) == 4)
    assert captured == [b"test_1", b"test_2", b"test_3", b"test_4"]


async def test_concurrent_publish(stream: str, producer: Producer, consumer: Consumer) -> None:
    captured: list[bytes] = []
    await consumer.subscribe(
        stream, callback=lambda message, message_context: captured.append(bytes(message))
    )

    await asyncio.gather(
        *(
            producer.send_wait(
                stream,
                RawMessage(b"test", publishing_id),
            )
            for publishing_id in range(1, 11)
        )
    )

    await wait_for(lambda: len(captured) == 10)
    assert captured == [b"test"] * 10


async def test_concurrent_publish_async(stream: str, producer: Producer, consumer: Consumer) -> None:
    captured: list[bytes] = []
    await consumer.subscribe(
        stream, callback=lambda message, message_context: captured.append(bytes(message))
    )

    await asyncio.gather(
        *(
            producer.send(
                stream,
                RawMessage(b"test", publishing_id),
            )
            for publishing_id in range(1, 11)
        )
    )

    await wait_for(lambda: len(captured) == 10)
    assert captured == [b"test"] * 10


async def test_send_async_confirmation(stream: str, producer: Producer) -> None:

    confirmed_messages: list[int] = []
    errored_messages: list[int] = []

    async def publish_with_ids(*ids):
        for publishing_id in ids:
            await producer.send(
                stream,
                RawMessage(f"test_{publishing_id}".encode(), publishing_id),
                on_publish_confirm=partial(
                    on_publish_confirm_client_callback,
                    confirmed_messages=confirmed_messages,
                    errored_messages=errored_messages,
                ),
            )

    await publish_with_ids(1, 2, 3)

    await wait_for(lambda: len(confirmed_messages) == 3)


# Checks if to different sends can be registered different callbacks
async def test_send_async_confirmation_on_different_callbacks(stream: str, producer: Producer) -> None:

    confirmed_messages: list[int] = []
    confirmed_messages2: list[int] = []
    errored_messages: list[int] = []

    async def publish_with_ids(*ids):
        for publishing_id in ids:
            await producer.send(
                stream,
                RawMessage(f"test_{publishing_id}".encode(), publishing_id),
                on_publish_confirm=partial(
                    on_publish_confirm_client_callback,
                    confirmed_messages=confirmed_messages,
                    errored_messages=errored_messages,
                ),
            )
            await producer.send(
                stream,
                RawMessage(f"test_{publishing_id}".encode(), publishing_id),
                on_publish_confirm=partial(
                    on_publish_confirm_client_callback2,
                    confirmed_messages=confirmed_messages2,
                    errored_messages=errored_messages,
                ),
            )

    await publish_with_ids(1, 2, 3)

    await wait_for(lambda: len(confirmed_messages) == 3)
    await wait_for(lambda: len(confirmed_messages2) == 3)


async def test_send_entry_subbatch_async_confirmation(stream: str, producer: Producer) -> None:

    confirmed_messages: list[int] = []
    errored_messages: list[int] = []

    async def publish_with_ids(*ids):
        entry_list = []
        for publishing_id in ids:
            entry_list.append(RawMessage(f"test_{publishing_id}".encode(), publishing_id))

        await producer.send_sub_entry(
            stream,
            entry_list,
            compression_type=CompressionType.Gzip,
            on_publish_confirm=partial(
                on_publish_confirm_client_callback,
                confirmed_messages=confirmed_messages,
                errored_messages=errored_messages,
            ),
        )

    await publish_with_ids(1, 2, 3)

    await wait_for(lambda: len(confirmed_messages) == 1)


async def test_producer_restart(stream: str, producer: Producer, consumer: Consumer) -> None:
    captured: list[bytes] = []
    await consumer.subscribe(
        stream, callback=lambda message, message_context: captured.append(bytes(message))
    )

    await producer.send_wait(stream, b"one")

    await producer.close()
    await producer.start()

    await producer.send_wait(stream, b"two")

    await wait_for(lambda: len(captured) == 2)
    assert captured == [b"one", b"two"]


async def test_publishing_sequence_superstream(
    super_stream: str, super_stream_producer: SuperStreamProducer, super_stream_consumer: SuperStreamConsumer
) -> None:
    captured: list[bytes] = []

    await super_stream_consumer.subscribe(
        callback=lambda message, message_context: captured.append(bytes(message)), decoder=amqp_decoder
    )

    async def publish_with_ids(*ids):
        for publishing_id in ids:
            amqp_message = AMQPMessage(
                body="a:{}".format(publishing_id),
            )

            await super_stream_producer.send(amqp_message)

    await publish_with_ids(1, 2, 3)

    await wait_for(lambda: len(captured) == 3)


async def test_publishing_sequence_superstream_key_routing(
    super_stream: str, super_stream_key_routing_producer: SuperStreamProducer, consumer: Consumer
) -> None:
    captured: list[bytes] = []

    await consumer.subscribe(
        stream="test-super-stream-0",
        callback=lambda message, message_context: captured.append(bytes(message)),
        decoder=amqp_decoder,
    )

    async def publish_with_ids(*ids):
        for publishing_id in ids:
            amqp_message = AMQPMessage(
                body="a:{}".format(publishing_id),
            )
            # will send to super_stream with routing key of 'key1'
            await super_stream_key_routing_producer.send(amqp_message)

    await publish_with_ids(1, 2, 3)

    await wait_for(lambda: len(captured) == 3)


async def test_publishing_sequence_superstream_binary(
    super_stream: str, super_stream_producer: SuperStreamProducer, super_stream_consumer: SuperStreamConsumer
) -> None:
    captured: list[bytes] = []

    await super_stream_consumer.subscribe(
        callback=lambda message, message_context: captured.append(bytes(message))
    )

    async def publish_with_ids(*ids):
        for _ in ids:

            await super_stream_producer.send(b"one")

    await publish_with_ids(1, 2, 3)

    await wait_for(lambda: len(captured) == 3)


async def test_publishing_sequence_superstream_with_callback(
    super_stream: str, super_stream_producer: SuperStreamProducer
) -> None:

    confirmed_messages: list[int] = []
    errored_messages: list[int] = []

    async def publish_with_ids(*ids):
        for publishing_id in ids:
            amqp_message = AMQPMessage(
                body="a:{}".format(publishing_id),
            )
            await super_stream_producer.send(
                amqp_message,
                on_publish_confirm=partial(
                    on_publish_confirm_client_callback,
                    confirmed_messages=confirmed_messages,
                    errored_messages=errored_messages,
                ),
            )

    await publish_with_ids(1, 2, 3)

    await wait_for(lambda: len(confirmed_messages) == 3)


async def test_producer_connection_broke(stream: str) -> None:

    connection_broke = False
    stream_disconnected = None
    producer_broke: Producer

    async def on_connection_closed(disconnection_info: DisconnectionErrorInfo) -> None:
        nonlocal connection_broke
        connection_broke = True
        nonlocal producer_broke

        nonlocal stream_disconnected
        stream_disconnected = disconnection_info.streams.pop()

        await producer_broke.close()

    producer_broke = Producer(
        "localhost",
        username="guest",
        password="guest",
        connection_closed_handler=on_connection_closed,
        connection_name="test-connection",
    )

    await producer_broke.start()
    asyncio.create_task(task_to_delete_connection("test-connection"))

    while connection_broke is False:
        try:
            await producer_broke.send(stream, b"one")
        # Connection broke
        except BaseException:
            break

    await producer_broke.close()

    assert connection_broke is True
    assert stream_disconnected == stream


async def test_producer_connection_broke_with_send_batch(stream: str) -> None:

    connection_broke = False
    stream_disconnected = None
    producer_broke: Producer

    async def on_connection_closed(disconnection_info: DisconnectionErrorInfo) -> None:

        nonlocal connection_broke
        connection_broke = True
        nonlocal producer_broke

        nonlocal stream_disconnected
        stream_disconnected = disconnection_info.streams.pop()

    producer_broke = Producer(
        "localhost",
        username="guest",
        password="guest",
        connection_closed_handler=on_connection_closed,
        connection_name="test-connection",
    )

    await producer_broke.start()
    asyncio.create_task(task_to_delete_connection("test-connection"))

    while connection_broke is False:
        batch = []
        for i in range(10000):
            amqp_message = AMQPMessage(
                body="hello: {}".format(i),
            )
            batch.append(amqp_message)
        try:
            if connection_broke is False:
                await producer_broke.send_batch(stream, batch)  # type: ignore
                batch.clear()
            else:
                break
        # Connection closed
        except BaseException:
            break

    await producer_broke.close()
    assert connection_broke is True
    assert stream_disconnected == stream


async def test_super_stream_producer_connection_broke(super_stream: str) -> None:

    connection_broke = False
    streams_disconnected: set[str] = set()
    producer_broke: Producer

    async def on_connection_closed(disconnection_info: DisconnectionErrorInfo) -> None:
        nonlocal connection_broke
        connection_broke = True
        nonlocal producer_broke

        nonlocal streams_disconnected
        for stream in disconnection_info.streams:
            streams_disconnected.add(stream)

    super_stream_producer_broke = SuperStreamProducer(
        "localhost",
        username="guest",
        password="guest",
        routing_extractor=routing_extractor_generic,
        routing=RouteType.Hash,
        connection_closed_handler=on_connection_closed,
        connection_name="test-connection",
        super_stream=super_stream,
    )

    await super_stream_producer_broke.start()

    asyncio.create_task(task_to_delete_connection("test-connection"))
    i = 0
    while connection_broke is False:
        amqp_message = AMQPMessage(
            body="hello: {}".format(i),
            application_properties={"id": "{}".format(i)},
        )
        i = i + 1
        # send is asynchronous
        try:
            if connection_broke is False:
                await super_stream_producer_broke.send(message=amqp_message)
            else:
                break
        # Connection closed
        except BaseException:
            break

    await super_stream_producer_broke.close()

    assert connection_broke is True
    assert "test-super-stream-0" in streams_disconnected
    assert "test-super-stream-1" in streams_disconnected
    assert "test-super-stream-2" in streams_disconnected


async def test_producer_connection_broke_with_reconnect(stream: str) -> None:

    connection_broke = False
    disconnected_once = False
    stream_disconnected = None
    producer_broke: Producer

    async def on_connection_closed(disconnection_info: DisconnectionErrorInfo) -> None:
        nonlocal connection_broke
        connection_broke = True
        nonlocal producer_broke

        nonlocal stream_disconnected
        # stream_disconnected = disconnection_info.streams.pop()

        for stream in disconnection_info.streams:
            stream_disconnected = stream
            await producer_broke.reconnect_stream(stream)

    producer_broke = Producer(
        "localhost",
        username="guest",
        password="guest",
        connection_closed_handler=on_connection_closed,
        connection_name="test-connection",
    )

    await producer_broke.start()
    asyncio.create_task(task_to_delete_connection("test-connection"))

    while True:
        try:
            await producer_broke.send(stream, b"one")
            if disconnected_once is True and connection_broke is True:
                connection_broke = False
                break
        # Connection broke wait for reconnection
        except BaseException:
            disconnected_once = True
            # wait for reconnection
            await asyncio.sleep(2)
            continue

    await producer_broke.close()

    assert disconnected_once is True
    assert connection_broke is False
    assert stream_disconnected == stream


async def test_super_stream_producer_connection_broke_with_reconnect(super_stream: str) -> None:

    connection_broke = False
    disconnected_once = False
    streams_disconnected: set[str] = set()
    producer_broke: Producer

    async def on_connection_closed(disconnection_info: DisconnectionErrorInfo) -> None:
        nonlocal connection_broke
        connection_broke = True
        nonlocal producer_broke

        nonlocal streams_disconnected
        for stream in disconnection_info.streams:
            streams_disconnected.add(stream)
            await super_stream_producer_broke.reconnect_stream(stream)

    super_stream_producer_broke = SuperStreamProducer(
        "localhost",
        username="guest",
        password="guest",
        routing_extractor=routing_extractor_generic,
        routing=RouteType.Hash,
        connection_closed_handler=on_connection_closed,
        connection_name="test-connection",
        super_stream=super_stream,
    )

    await super_stream_producer_broke.start()

    asyncio.create_task(task_to_delete_connection("test-connection"))
    i = 0
    while True:
        amqp_message = AMQPMessage(
            body="hello: {}".format(i),
            application_properties={"id": "{}".format(i)},
        )
        i = i + 1
        # send is asynchronous
        try:
            await super_stream_producer_broke.send(message=amqp_message)
            if disconnected_once is True and connection_broke is True:
                connection_broke = False
                break
            # Connection broke wait for reconnection
        except BaseException:
            disconnected_once = True
            # wait for reconnection
            await asyncio.sleep(2)
            continue

    await super_stream_producer_broke.close()

    assert disconnected_once is True
    assert connection_broke is False
    assert "test-super-stream-0" in streams_disconnected
    assert "test-super-stream-1" in streams_disconnected
    assert "test-super-stream-2" in streams_disconnected
