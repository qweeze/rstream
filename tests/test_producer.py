# Copyright 2023 VMware, Inc. All Rights Reserved.
# SPDX-License-Identifier: MIT

import asyncio
from functools import partial

import pytest

from rstream import (
    CompressionType,
    Consumer,
    Producer,
    RawMessage,
    exceptions,
)

from .util import (
    on_publish_confirm_client_callback,
    wait_for,
)

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
    await consumer.subscribe(stream, callback=captured.append)

    assert await producer.send_wait(stream, b"one") == 1
    assert await producer.send_batch(stream, [b"two", b"three"]) == [2, 3]
    await wait_for(lambda: len(captured) == 3)
    assert captured == [b"one", b"two", b"three"]


async def test_publishing_sequence_subbatching_nocompression(
    stream: str, producer: Producer, consumer: Consumer
) -> None:
    captured: list[bytes] = []

    await consumer.subscribe(stream, callback=captured.append)

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

    await consumer.subscribe(stream, callback=captured.append)

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

    await consumer.subscribe(stream, callback=captured.append)

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

    await consumer.subscribe(stream, callback=captured.append)

    await producer.send(stream, b"one")
    await producer.send(stream, b"two")
    await producer.send(stream, b"three")

    await wait_for(lambda: len(captured) == 3)
    assert captured == [b"one", b"two", b"three"]


async def test_publish_deduplication(stream: str, producer: Producer, consumer: Consumer) -> None:
    captured: list[bytes] = []
    await consumer.subscribe(stream, callback=captured.append)

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
    await consumer.subscribe(stream, callback=captured.append)

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
    await consumer.subscribe(stream, callback=captured.append)

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
    await consumer.subscribe(stream, callback=captured.append)

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
    await consumer.subscribe(stream, callback=captured.append)

    await producer.send_wait(stream, b"one")

    await producer.close()
    await producer.start()

    await producer.send_wait(stream, b"two")

    await wait_for(lambda: len(captured) == 2)
    assert captured == [b"one", b"two"]
