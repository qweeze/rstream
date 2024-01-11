import asyncio
import time

from rstream import AMQPMessage, OnClosedErrorInfo, Producer

STREAM = "my-test-stream"
MESSAGES = 10000000
meta_data_updated = False


async def publish():
    async def on_metadata_update(metadata_update_info: OnClosedErrorInfo) -> None:

        print(
            "metadata changed for stream : "
            + str(metadata_update_info.streams[0])
            + " with code: "
            + metadata_update_info.reason
        )

        global meta_data_updated
        meta_data_updated = True
        await producer.close()

    print("creating Producer")
    # producer will be closed at the end by the async context manager
    # both if connection is still alive or not
    async with Producer(
        "34.89.82.143",
        username="default_user_dihAqY5mlRseK375uAK",
        password="SvPRDs1ba-YXBS6by1Y3YCUcoCXf_jAE",
        load_balancer_mode=True,
        on_close_handler=on_metadata_update,
    ) as producer:

        # create a stream if it doesn't already exist
        await producer.create_stream(STREAM, exists_ok=True)

        # sending a million of messages in AMQP format
        start_time = time.perf_counter()

        print("Sending MESSAGES")
        for i in range(MESSAGES):
            amqp_message = AMQPMessage(
                body="hello: {}".format(i),
            )
            # send is asynchronous
            global meta_data_updated
            if meta_data_updated is False:
                await producer.send(stream=STREAM, message=amqp_message)
            else:
                producer = Producer(
                    "34.89.82.143",
                    username="default_user_dihAqY5mlRseK375uAK",
                    password="SvPRDs1ba-YXBS6by1Y3YCUcoCXf_jAE",
                    load_balancer_mode=True,
                    on_close_handler=on_metadata_update,
                )

                await producer.start()
                meta_data_updated = False
                continue

            if i % 100000 == 0:
                print("sent 10000 messages")

    end_time = time.perf_counter()
    print(f"Sent {MESSAGES} messages in {end_time - start_time:0.4f} seconds")


asyncio.run(publish())
