import pytest

from rstream import Producer


pytestmark = pytest.mark.asyncio


async def test_publish(producer: Producer, stream: str) -> None:
    assert await producer.publish(stream, b'one') == 1
    assert await producer.publish_batch(stream, [b'two', b'three']) == [2, 3]
    await producer.close()
