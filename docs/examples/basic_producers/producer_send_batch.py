import asyncio

from rstream import AMQPMessage, Producer

STREAM = "my-test-stream"
LOOP = 1000
BATCH = 1000


async def publish():

    async with Producer("localhost", username="guest", password="guest") as producer:
<<<<<<< HEAD
        # create a stream if it doesn't already exist
        await producer.create_stream(STREAM, exists_ok=True)

        # sending a million of messages in AMQP format
=======
        # create a strem if it doesn't already exist
        await producer.create_stream(STREAM, exists_ok=True)

        # sending a milion of messages in AMQP format
>>>>>>> 55778554d17244679f955868c2f02b07514c7352
        for j in range(LOOP):
            messages = []
            for i in range(BATCH):
                amqp_message = AMQPMessage(
                    body="hello: {}".format(i),
                )
                messages.append(amqp_message)
            # send_batch is synchronous. will wait till termination
            await producer.send_batch(stream=STREAM, batch=messages)


asyncio.run(publish())
