import asyncio
import ssl
import time

from rstream import AMQPMessage, Producer

STREAM = "my-test-stream"
LOOP = 10000
BATCH = 100


async def publish():
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    # put the root certificate of the ca
    ssl_context.load_verify_locations("/yourpath/ca_certificate.pem")

    async with Producer(
        "localhost", username="guest", password="guest", port=5551, ssl_context=ssl_context
    ) as producer:
        # create a stream if it doesn't already exist
        await producer.create_stream(STREAM, exists_ok=True)

        start_time = time.perf_counter()
        # sending a million of messages in AMQP format
        for j in range(LOOP):
            messages = []
            for i in range(BATCH):
                amqp_message = AMQPMessage(
                    body=bytes("hello: {}".format(i), "utf-8"),
                )
                messages.append(amqp_message)
            # send_batch is synchronous. will wait till termination
            await producer.send_batch(stream=STREAM, batch=messages)

        end_time = time.perf_counter()
        print(f"Sent 1000000 messages in {end_time - start_time:0.4f} seconds")

        # callbacks live in the same scope of Producer so we need to wait till the messages have been confirmed
        # before exiting Producer scope
        await asyncio.sleep(2)


asyncio.run(publish())
