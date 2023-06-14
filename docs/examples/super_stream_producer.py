import asyncio

from rstream import AMQPMessage, SuperStreamProducer, RouteType


async def routing_extractor(message: AMQPMessage) -> str:
    return message.application_properties['id']


async def publish():
    async with SuperStreamProducer('localhost', username='guest', password='guest',
                                   routing_extractor=routing_extractor,
                                   routing=RouteType.Hash,
                                   super_stream='invoices',
                                   ) as super_stream_producer:
        for i in range(100):
            amqp_message = AMQPMessage(
                body='hello: {}'.format(i),
                application_properties={'id': '{}'.format(i)},
            )
            await super_stream_producer.send(amqp_message)

asyncio.run(publish())
