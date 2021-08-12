from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass
from functools import partial
from typing import (
    Annotated,
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Optional,
    Union,
)

from . import constants, schema
from .client import Client, ClientPool

CB = Annotated[
    Callable[[bytes], Union[None, Awaitable[None]]],
    'Raw message callback type'
]


@dataclass
class _Subscriber:
    stream: str
    subscription_id: int
    reference: str
    client: Client
    callback: CB
    offset: int = 0


class Consumer:
    def __init__(
        self,
        host: str,
        port: int = 5552,
        *,
        vhost: str = '/',
        username: str,
        password: str,
        frame_max: int = 1 * 1024 * 1024,
        heartbeat: int = 60,
    ):
        self._pool = ClientPool(
            host,
            port,
            vhost=vhost,
            username=username,
            password=password,
            frame_max=frame_max,
            heartbeat=heartbeat,
        )
        self._default_client: Optional[Client] = None
        self._clients: dict[str, Client] = {}
        self._subscribers: dict[str, _Subscriber] = {}
        self._stop_event = asyncio.Event()

    @property
    def default_client(self) -> Client:
        if self._default_client is None:
            raise ValueError('Consumer is not started')
        return self._default_client

    async def __aenter__(self) -> Consumer:
        await self.start()
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.close()

    async def start(self) -> None:
        self._default_client = await self._pool.get()

    async def close(self) -> None:
        for subscriber in self._subscribers.values():
            await self.unsubscribe(subscriber.reference)
            await self.store_offset(subscriber.reference)

        self._subscribers.clear()

        await self._pool.close()
        self._clients.clear()
        self._default_client = None

        self._stop_event.set()

    async def run(self) -> None:
        await self._stop_event.wait()

    async def _get_or_create_client(self, stream: str) -> Client:
        if stream not in self._clients:
            leader, replicas = await self.default_client.query_leader_and_replicas(stream)
            broker = random.choice(replicas) if replicas else leader
            self._clients[stream] = await self._pool.get((broker.host, broker.port))

        return self._clients[stream]

    async def _create_subscriber(
        self,
        stream: str,
        subscirber_name: Optional[str],
        callback: CB,
    ) -> _Subscriber:
        client = await self._get_or_create_client(stream)

        # We can have multiple subscribers sharing same connection, so their ids must be distinct
        subscription_id = len([s for s in self._subscribers.values() if s.client is client]) + 1
        reference = subscirber_name or f'{stream}_subscriber_{subscription_id}'
        subscriber = self._subscribers[reference] = _Subscriber(
            stream=stream,
            subscription_id=subscription_id,
            client=client,
            reference=reference,
            callback=callback,
        )
        return subscriber

    async def subscribe(
        self,
        stream: str,
        callback: CB,
        *,
        offset_type: constants.OffsetType = constants.OffsetType.offset,
        offset: Optional[int] = None,
        initial_credit: int = 10,
        properties: Optional[dict[str, Any]] = None,
        subscirber_name: Optional[str] = None,
    ) -> str:
        subscriber = await self._create_subscriber(stream, subscirber_name, callback)

        if offset is None:
            subscriber.offset = await subscriber.client.query_offset(
                stream,
                subscriber.reference,
            )
        else:
            subscriber.offset = offset

        subscriber.client.add_handler(
            schema.Deliver,
            partial(self._on_deliver, subscriber=subscriber),
            name=subscriber.reference,
        )
        await subscriber.client.subscribe(
            stream=stream,
            subscription_id=subscriber.subscription_id,
            offset=subscriber.offset,
            offset_type=offset_type,
            initial_credit=initial_credit,
            properties=properties,
        )

        return subscriber.reference

    async def unsubscribe(self, subscirber_name: str) -> None:
        subscriber = self._subscribers[subscirber_name]
        subscriber.client.remove_handler(
            schema.Deliver,
            name=subscriber.reference,
        )
        await subscriber.client.unsubscribe(subscriber.subscription_id)

    async def iterator(
        self,
        stream: str,
        *,
        offset_type: constants.OffsetType = constants.OffsetType.offset,
        offset: Optional[int] = None,
        initial_credit: int = 10,
        properties: Optional[dict[str, Any]] = None,
        subscirber_name: Optional[str] = None,
    ) -> AsyncIterator[bytes]:
        queue: asyncio.Queue[bytes] = asyncio.Queue()
        await self.subscribe(
            stream,
            callback=queue.put_nowait,
            offset_type=offset_type,
            offset=offset,
            initial_credit=initial_credit,
            properties=properties,
            subscirber_name=subscirber_name,
        )
        while not self._stop_event.is_set():
            yield await queue.get()

    async def store_offset(self, subscirber_name: str) -> None:
        subscriber = self._subscribers[subscirber_name]
        await subscriber.client.store_offset(
            stream=subscriber.stream,
            reference=subscriber.reference,
            offset=subscriber.offset,
        )

    async def _on_deliver(self, frame: schema.Deliver, subscriber: _Subscriber) -> None:
        if frame.subscription_id != subscriber.subscription_id:
            return

        await subscriber.client.credit(subscriber.subscription_id, 1)

        for message in frame.get_messages():
            maybe_coro = subscriber.callback(message)
            if maybe_coro is not None:
                await maybe_coro

            subscriber.offset += 1

    async def create_stream(self, stream: str, arguments: Optional[dict[str, Any]] = None) -> None:
        await self.default_client.create_stream(stream, arguments)

    async def delete_stream(self, stream: str) -> None:
        if stream in self._subscribers:
            subscriber = self._subscribers[stream]
            await self.unsubscribe(subscriber.reference)
            del self._subscribers[stream]

        await self.default_client.delete_stream(stream)

    async def stream_exists(self, stream: str) -> bool:
        return await self.default_client.stream_exists(stream)
