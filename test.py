import asyncio
import time

from rstream import Producer, AMQPMessage, ConfirmationStatus, CompressionType

global counter


def _on_publish_confirm_client(confirmation: ConfirmationStatus) -> None:
    if confirmation.is_confirmed:
        global counter
        counter += 1
        if counter % 100 == 0:
            print("confirmed: " + str(counter))

    else:
        print("message id: " + str(confirmation.message_id) + " is not confirmed")


async def publish():
    global counter
    counter = 0
    sent = 0
    async with Producer('localhost', username='guest', password='guest') as producer:
        await producer.delete_stream('mystream', missing_ok=True)
        await producer.create_stream('mystream', exists_ok=True)

        messages = []
        for i in range(10000):
            amqp_message = AMQPMessage(
                body='a:{}'.format(i),
            )
            messages.append(amqp_message)
            sent += 1

        await producer.send_batch('mystream', messages)

        await producer.send_sub_entry('mystream', compression_type=CompressionType.Gzip,
                                      publishing_messages=messages)

        await producer.send_sub_entry('mystream', compression_type=CompressionType.No,
                                      publishing_messages=messages)

        await producer.send_sub_entry('mystream', compression_type=CompressionType.Gzip,
                                      publishing_messages=messages)
    print("waiting...")
    await asyncio.sleep(5)
    print("closing...")
    await producer.close()
    print("closed...")


asyncio.run(publish())

print("done")
