import asyncio

import pytest

from rstream import Consumer, Message, Producer

from .util import wait_for

pytestmark = pytest.mark.asyncio


async def test_publishing_sequence(stream: str, producer: Producer, consumer: Consumer) -> None:
    captured: list[bytes] = []
    await consumer.subscribe(stream, callback=captured.append)

    assert await producer.publish(stream, b'one') == 1
    assert await producer.publish_batch(stream, [b'two', b'three']) == [2, 3]

    await wait_for(lambda: len(captured) == 3)
    assert captured == [b'one', b'two', b'three']


async def test_publish_deduplication(stream: str, producer: Producer, consumer: Consumer) -> None:
    captured: list[bytes] = []
    await consumer.subscribe(stream, callback=captured.append)

    async def publish_with_ids(*ids):
        for publishing_id in ids:
            await producer.publish(
                stream,
                Message(f'test_{publishing_id}'.encode(), publishing_id),
            )

    await publish_with_ids(1, 2, 3)
    await publish_with_ids(1, 2, 3)

    await wait_for(lambda: len(captured) == 3)
    assert captured == [b'test_1', b'test_2', b'test_3']

    await publish_with_ids(2, 3, 4)

    await wait_for(lambda: len(captured) == 4)
    assert captured == [b'test_1', b'test_2', b'test_3', b'test_4']


async def test_concurrent_publish(stream: str, producer: Producer, consumer: Consumer) -> None:
    captured: list[bytes] = []
    await consumer.subscribe(stream, callback=captured.append)

    await asyncio.gather(*(
        producer.publish(
            stream,
            Message(b'test', publishing_id),
        ) for publishing_id in range(1, 11)
    ))

    await wait_for(lambda: len(captured) == 10)
    assert captured == [b'test'] * 10


async def test_producer_restart(stream: str, producer: Producer, consumer: Consumer) -> None:
    captured: list[bytes] = []
    await consumer.subscribe(stream, callback=captured.append)

    await producer.publish(stream, b'one')

    await producer.close()
    await producer.start()

    await producer.publish(stream, b'two')

    await wait_for(lambda: len(captured) == 2)
    assert captured == [b'one', b'two']
