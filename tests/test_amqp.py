import pytest

from rstream import (
    AMQPMessage,
    Consumer,
    Producer,
    amqp_decoder,
)

pytestmark = pytest.mark.asyncio


async def test_amqp_message(stream: str, consumer: Consumer, producer: Producer) -> None:
    amqp_message = AMQPMessage(
        properties={"subject": "test-subject"},
        annotations={"test": 42},
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
    assert incoming_amqp_message.body == amqp_message.body
    assert incoming_amqp_message.properties == amqp_message.properties
    assert incoming_amqp_message.annotations == amqp_message.annotations
