import asyncio
import time

from rstream import (
    AMQPMessage,
    OnClosedErrorInfo,
    RouteType,
    StreamDoesNotExist,
    SuperStreamProducer,
)

SUPER_STREAM = "invoices"
MESSAGES = 100000000
producer_closed = False

# this value will be hashed using mumh3 hashing algorithm to decide the partition resolution for the message
async def routing_extractor(message: AMQPMessage) -> str:
    return message.application_properties["id"]


async def publish():

    # SuperStreamProducer wraps a Producer
    async with SuperStreamProducer(
        "localhost",
        username="test",
        password="test",
        routing_extractor=routing_extractor,
        routing=RouteType.Hash,
        super_stream=SUPER_STREAM,
        load_balancer_mode=True,
    ) as super_stream_producer:
        # Sending a million messages

        start_time = time.perf_counter()
        for i in range(MESSAGES):
            amqp_message = AMQPMessage(
                body="hello: {}".format(i),
                application_properties={"id": "{}".format(i)},
            )
            global producer_closed

            if producer_closed is False:
                try:
                    await super_stream_producer.send(amqp_message)
                except Exception as e:
                    # give some time to the reconnect_stream to reconnect
                    print("error sending message: {}".format(e))
                    producer_closed = False
                    continue
            else:
                producer_closed = False
                continue
            if i % 100000 == 0:
                print(f"Published {i} messages to super stream: {SUPER_STREAM}")
                await asyncio.sleep(2)

        end_time = time.perf_counter()
        print(
            f"Sent {MESSAGES} messages in {end_time - start_time:0.4f} seconds to super stream: {SUPER_STREAM}"
        )


asyncio.run(publish())
