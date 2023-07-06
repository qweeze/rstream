import asyncio

from rstream import AMQPMessage, Producer

STREAM = "my-test-stream"
MESSAGES = 1000000


async def publish():

    async with Producer("localhost", username="guest", password="guest") as producer:
        # create a stream if it doesn't already exist
        await producer.create_stream(STREAM, exists_ok=True)

<<<<<<< HEAD
        # sending a million of messages in AMQP format
=======
        # sending a milion of messages in AMQP format
>>>>>>> 55778554d17244679f955868c2f02b07514c7352
        for i in range(MESSAGES):
            amqp_message = AMQPMessage(
                body="hello: {}".format(i),
            )
            # send is asynchronous
            await producer.send(stream=STREAM, message=amqp_message)


asyncio.run(publish())
