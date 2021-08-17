
import pytest
from uamqp.message import Message, MessageProperties

from rstream import Consumer, Producer

pytestmark = pytest.mark.asyncio


async def test_amqp_message(stream: str, consumer: Consumer, producer: Producer) -> None:
    message = Message(
        properties=MessageProperties(subject='test-subject'),
        body='test-body',
    )
    await producer.publish(stream, message.encode_message())

    received_message = None

    def callback(raw_msg):
        nonlocal received_message
        received_message = Message.decode_from_bytes(raw_msg)
        consumer.stop()

    await consumer.subscribe(stream, callback=callback)
    await consumer.run()

    assert list(received_message.get_data()) == [b'test-body']
    assert received_message.properties.subject == b'test-subject'
