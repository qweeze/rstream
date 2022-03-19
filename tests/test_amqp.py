import pytest
import uamqp.message

from rstream import (
    AMQPMessage,
    Consumer,
    Producer,
    amqp_decoder,
)

pytestmark = pytest.mark.asyncio


async def test_amqp_message(stream: str, consumer: Consumer, producer: Producer) -> None:
    amqp_message = AMQPMessage(
        properties=uamqp.message.MessageProperties(subject=b"test-subject"),
        annotations={b"test": 42},
        body="test-body",
    )
    await producer.publish(stream, amqp_message)

    incoming_amqp_message = None

    def callback(msg: AMQPMessage):
        nonlocal incoming_amqp_message
        incoming_amqp_message = msg
        consumer.stop()

    await consumer.subscribe(stream, callback=callback, decoder=amqp_decoder)
    await consumer.run()

    assert isinstance(incoming_amqp_message, AMQPMessage)
    assert list(incoming_amqp_message.get_data()) == list(amqp_message.get_data())
    assert incoming_amqp_message.properties.subject == amqp_message.properties.subject
    assert incoming_amqp_message.annotations == amqp_message.annotations
