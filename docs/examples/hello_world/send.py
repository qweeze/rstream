import asyncio

from rstream import Producer

STREAM_NAME = "hello-python-stream"


async def send():
    async with Producer("localhost", username="guest", password="guest") as producer:
        await producer.create_stream(STREAM_NAME, exists_ok=True)

        await producer.send(stream=STREAM_NAME, message=b"Hello World!")


asyncio.run(send())
