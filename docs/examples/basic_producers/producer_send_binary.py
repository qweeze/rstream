import asyncio

from rstream import AMQPMessage, Producer

STREAM = "my-test-stream"
MESSAGES = 1000000


async def publish():

    async with Producer("localhost", username="guest", password="guest") as producer:
        # create a stream if it doesn't already exist
        await producer.create_stream(STREAM, exists_ok=True)

<<<<<<< HEAD
        # sending a million of messages in binary format
        # note that this is not compatible with other clients (e.g. Java,.NET)
        # since they expect messages in AMQP 1.0
=======
        # sending a milion of messages in AMQP format
>>>>>>> 55778554d17244679f955868c2f02b07514c7352
        for i in range(MESSAGES):

            # send is asynchronous
            await producer.send(stream=STREAM, message=b"hello")


asyncio.run(publish())
