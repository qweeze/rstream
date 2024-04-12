import asyncio
import time

from rstream import AMQPMessage, Producer

STREAM = "my-test-stream"
MESSAGES = 200


async def filter_value_extractor(message: AMQPMessage) -> str:
    return message.application_properties["region"]


async def publish():
    async with Producer(
        "localhost", username="guest", password="guest", filter_value_extractor=filter_value_extractor
    ) as producer:
        # create a stream if it doesn't already exist
        await producer.create_stream(STREAM, exists_ok=True)

        start_time = time.perf_counter()

        # sending 200 messages with filtering New York
        for i in range(MESSAGES):
            application_properties = {
                "region": "New York",
            }
            amqp_message = AMQPMessage(
                body="hello: {}".format(i),
                application_properties=application_properties,
            )
            # send is asynchronous
            await producer.send(stream=STREAM, message=amqp_message)

        # wait a bit to ensure all messages will go to a chunk
        # don't do this in production. This is only for testing purposes
        # it is to force the bloom filter to be created
        await asyncio.sleep(2)

        # sending 200 messages with filtering California
        for i in range(MESSAGES):
            application_properties = {
                "region": "California",
            }
            amqp_message = AMQPMessage(
                body="hello: {}".format(i),
                application_properties=application_properties,
            )
            # send is asynchronous
            await producer.send(stream=STREAM, message=amqp_message)

        end_time = time.perf_counter()
        print(f"Sent {MESSAGES} messages in {end_time - start_time:0.4f} seconds")


asyncio.run(publish())
