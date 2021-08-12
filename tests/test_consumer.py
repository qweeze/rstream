import pytest
import asyncio

from rstream import Producer, Consumer


pytestmark = pytest.mark.asyncio


async def test_consume(stream: str, consumer: Consumer, producer: Producer) -> None:
    captured: list[bytes] = []
    await consumer.subscribe(stream, callback=captured.append)
    assert await producer.publish(stream, b'one') == 1
    assert await producer.publish_batch(stream, [b'two', b'three']) == [2, 3]
    await asyncio.sleep(0.1)
    assert captured == [b'one', b'two', b'three']
