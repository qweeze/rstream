import asyncio
import time

from rstream import (
    AMQPMessage,
    OnClosedErrorInfo,
    RouteType,
    SuperStreamProducer,
)

SUPER_STREAM = "invoices"
MESSAGES = 10000000
producer_closed = False

# this value will be hashed using mumh3 hashing algorithm to decide the partition resolution for the message
async def routing_extractor(message: AMQPMessage) -> str:
    return message.application_properties["id"]


async def publish():
    async def on_metadata_update(on_closed_info: OnClosedErrorInfo) -> None:

        print("im here")
        if on_closed_info.reason == "MetaData Update":
            print(
                "metadata changed for stream : "
                + str(on_closed_info.streams[0])
                + " with code: "
                + on_closed_info.reason
            )

        else:
            print(
                "connection has been closed from stream: "
                + str(on_closed_info.streams)
                + " for reason: "
                + str(on_closed_info.reason)
            )

        global producer_closed
        producer_closed = True
        await super_stream_producer.reconnect_stream(on_closed_info.streams[0])

    # SuperStreamProducer wraps a Producer
    async with SuperStreamProducer(
        "34.89.82.143",
        username="default_user_dihAqY5mlRseK375uAK",
        password="SvPRDs1ba-YXBS6by1Y3YCUcoCXf_jAE",
        routing_extractor=routing_extractor,
        routing=RouteType.Hash,
        super_stream=SUPER_STREAM,
        on_close_handler=on_metadata_update,
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
                    await asyncio.sleep(3)
                    continue
            else:
                producer_closed = False
                continue
            if i % 100000 == 0:
                print(f"Published {i} messages to super stream: {SUPER_STREAM}")

        end_time = time.perf_counter()
        print(
            f"Sent {MESSAGES} messages in {end_time - start_time:0.4f} seconds to super stream: {SUPER_STREAM}"
        )


asyncio.run(publish())
