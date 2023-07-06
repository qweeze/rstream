import asyncio

from rstream import (
    AMQPMessage,
    ConfirmationStatus,
    Producer,
)

STREAM = "my-test-stream"
<<<<<<< HEAD
LOOP = 10_000
BATCH = 100


def _on_publish_confirm_client(confirmation: ConfirmationStatus) -> None:
    if confirmation.is_confirmed:
        print("message id: {} is confirmed".format(confirmation.message_id))
    else:
        print("message id: {} not confirmed. Response code {}".format(confirmation.message_id,
                                                                      confirmation.response_code))


async def publish():
    async with Producer("localhost", username="guest", password="guest") as producer:
        # create a stream if it doesn't already exist
=======
LOOP = 1000
BATCH = 1000


def _on_publish_confirm_client(confirmation: ConfirmationStatus) -> None:

    if confirmation.is_confirmed == True:
        print("message id: " + str(confirmation.message_id) + " is confirmed")
    else:
        print("message id: " + str(confirmation.message_id) + " is not confirmed")


async def publish():

    async with Producer("localhost", username="guest", password="guest") as producer:
        # create a strem if it doesn't already exist
>>>>>>> 55778554d17244679f955868c2f02b07514c7352
        await producer.create_stream(STREAM, exists_ok=True)

        # sending a million of messages in AMQP format
        for j in range(LOOP):
            messages = []
            for i in range(BATCH):
                amqp_message = AMQPMessage(
                    body="hello: {}".format(i),
                )
                messages.append(amqp_message)
            # send_batch is synchronous. will wait till termination
            await producer.send_batch(
                stream=STREAM, batch=messages, on_publish_confirm=_on_publish_confirm_client
            )

        # callbacks live in the same scope of Producer so we need to wait till the messages have been confirmed
        # before exiting Producer scope
        await asyncio.sleep(2)


asyncio.run(publish())
