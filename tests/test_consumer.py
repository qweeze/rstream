import asyncio
import time

import pytest

from rstream import (
    Consumer,
    OffsetType,
    Producer,
    exceptions,
)

from .util import wait_for

pytestmark = pytest.mark.asyncio


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
    await consumer.subscribe(stream, callback=captured.append)
    assert await producer.publish(stream, b"one") == 1
    assert await producer.publish_batch(stream, [b"two", b"three"]) == [2, 3]

    await wait_for(lambda: len(captured) >= 3)
    assert captured == [b"one", b"two", b"three"]


async def test_offset_type_first(stream: str, consumer: Consumer, producer: Producer) -> None:
    captured: list[bytes] = []
    await consumer.subscribe(
        stream,
        callback=captured.append,
        offset_type=OffsetType.FIRST,
    )
    messages = [str(i).encode() for i in range(1, 11)]
    await producer.publish_batch(stream, messages)

    await wait_for(lambda: len(captured) >= 10)
    assert captured == messages


async def test_offset_type_offset(stream: str, consumer: Consumer, producer: Producer) -> None:
    captured: list[bytes] = []
    await consumer.subscribe(
        stream,
        callback=captured.append,
        offset_type=OffsetType.OFFSET,
        offset=7,
    )
    messages = [str(i).encode() for i in range(1, 11)]
    await producer.publish_batch(stream, messages)

    await wait_for(lambda: len(captured) >= 3)
    assert captured == messages[7:]


async def test_offset_type_last(stream: str, consumer: Consumer, producer: Producer) -> None:
    await consumer.store_offset(stream, "test-subscriber", offset=7)
    captured: list[bytes] = []
    await consumer.subscribe(
        stream,
        callback=captured.append,
        offset_type=OffsetType.LAST,
        subscriber_name="test-subscriber",
    )

    messages = [str(i).encode() for i in range(1, 11)]
    await producer.publish_batch(stream, messages)

    await wait_for(lambda: len(captured) >= 4)
    assert captured == messages[6:]


async def test_offset_type_next(stream: str, consumer: Consumer, producer: Producer) -> None:
    await consumer.store_offset(stream, "test-subscriber", offset=7)
    captured: list[bytes] = []
    await consumer.subscribe(
        stream,
        callback=captured.append,
        offset_type=OffsetType.NEXT,
        subscriber_name="test-subscriber",
    )

    messages = [str(i).encode() for i in range(1, 11)]
    await producer.publish_batch(stream, messages)

    await wait_for(lambda: len(captured) >= 3)
    assert captured == messages[7:]


async def test_offset_type_timestamp(stream: str, consumer: Consumer, producer: Producer) -> None:
    captured: list[bytes] = []
    now_ms = int(time.time() * 1000)
    await consumer.subscribe(
        stream,
        callback=captured.append,
        offset_type=OffsetType.TIMESTAMP,
        offset=now_ms + 100,
    )
    await producer.publish(stream, b"one")
    await asyncio.sleep(0.1)
    await producer.publish(stream, b"two")

    await wait_for(lambda: len(captured) >= 1)
    assert captured == [b"two"]


async def test_consume_with_resubscribe(stream: str, consumer: Consumer, producer: Producer) -> None:
    captured: list[bytes] = []
    subscriber_name = await consumer.subscribe(stream, callback=captured.append)
    await producer.publish(stream, b"one")
    await wait_for(lambda: len(captured) >= 1)

    await consumer.unsubscribe(subscriber_name)
    await consumer.subscribe(stream, callback=captured.append, offset_type=OffsetType.NEXT)

    await producer.publish(stream, b"two")
    await wait_for(lambda: len(captured) >= 2)
    assert captured == [b"one", b"two"]

async def test_consume_with_resubscribe_on_last(stream: str, consumer: Consumer, producer: Producer) -> None:
    captured: list[bytes] = []
    subscriber_name = await consumer.subscribe(stream, callback=captured.append)
    await producer.publish(stream, b"one")
    await wait_for(lambda: len(captured) >= 1)

    await consumer.unsubscribe(subscriber_name)
    await consumer.subscribe(stream, callback=captured.append, offset_type=OffsetType.LAST)

    await producer.publish(stream, b"two")
    await wait_for(lambda: len(captured) >= 3)
    assert captured == [b"one", b"one", b"two"]

async def test_consume_with_restart(stream: str, consumer: Consumer, producer: Producer) -> None:
    captured: list[bytes] = []
    await consumer.subscribe(stream, callback=captured.append)
    await producer.publish(stream, b"one")
    await wait_for(lambda: len(captured) >= 1)

    await consumer.close()
    await consumer.start()
    await consumer.subscribe(stream, callback=captured.append, offset_type=OffsetType.NEXT)

    await producer.publish(stream, b"two")
    await wait_for(lambda: len(captured) >= 2)
    assert captured == [b"one", b"two"]


async def test_consume_multiple_streams(consumer: Consumer, producer: Producer) -> None:
    streams = ["stream1", "stream2", "stream3"]
    try:
        await asyncio.gather(*(consumer.create_stream(stream) for stream in streams))

        captured: list[bytes] = []
        await asyncio.gather(*(consumer.subscribe(stream, callback=captured.append) for stream in streams))

        await asyncio.gather(*(producer.publish(stream, b"test") for stream in streams))

        await wait_for(lambda: len(captured) >= 3)
        assert captured == [b"test", b"test", b"test"]

    finally:
        await producer.close()
        await asyncio.gather(*(consumer.delete_stream(stream) for stream in streams))
