import asyncio


async def wait_for(condition, timeout=1):
    async def _wait():
        while not condition():
            await asyncio.sleep(0.01)

    await asyncio.wait_for(_wait(), timeout)
