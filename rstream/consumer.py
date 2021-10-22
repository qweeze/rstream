from __future__ import annotations

import asyncio
import random
import ssl
from dataclasses import dataclass
from functools import partial
from typing import (
    Annotated,
    Any,
    Awaitable,
    Callable,
    Iterator,
    Optional,
    TypeVar,
    Union,
)

from . import exceptions, schema
from .client import Client, ClientPool
from .constants import OffsetType

MT = TypeVar("MT")
CB = Annotated[Callable[[MT], Union[None, Awaitable[None]]], "Message callback type"]


@dataclass
class _Subscriber:
    stream: str
    subscription_id: int
    reference: str
    client: Client
    callback: CB[Any]
    decoder: Callable[[bytes], Any]
    offset_type: OffsetType
    offset: int


class Consumer:
    def __init__(
        self,
        host: str,
        port: int = 5552,
        *,
        ssl_context: Optional[ssl.SSLContext] = None,
        vhost: str = "/",
        username: str,
        password: str,
        frame_max: int = 1 * 1024 * 1024,
        heartbeat: int = 60,
    ):
        self._pool = ClientPool(
            host,
            port,
            ssl_context=ssl_context,
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
        self._lock = asyncio.Lock()

    @property
    def default_client(self) -> Client:
        if self._default_client is None:
            raise ValueError("Consumer is not started")
        return self._default_client

    async def __aenter__(self) -> Consumer:
        await self.start()
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.close()

    async def start(self) -> None:
        self._default_client = await self._pool.get()

    def stop(self) -> None:
        self._stop_event.set()

    async def close(self) -> None:
        self.stop()

        for subscriber in list(self._subscribers.values()):
            await self.unsubscribe(subscriber.reference)
            await self.store_offset(subscriber.stream, subscriber.reference, subscriber.offset)

        self._subscribers.clear()

        await self._pool.close()
        self._clients.clear()
        self._default_client = None

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
        subscriber_name: Optional[str],
        callback: CB[MT],
        decoder: Optional[Callable[[bytes], Any]],
        offset_type: OffsetType,
        offset: Optional[int],
    ) -> _Subscriber:
        client = await self._get_or_create_client(stream)

        # We can have multiple subscribers sharing same connection, so their ids must be distinct
        subscription_id = len([s for s in self._subscribers.values() if s.client is client]) + 1
        reference = subscriber_name or f"{stream}_subscriber_{subscription_id}"
        decoder = decoder or (lambda x: x)

        if offset_type in (OffsetType.LAST, OffsetType.NEXT):
            offset = await self.query_offset(stream, reference)

        subscriber = self._subscribers[reference] = _Subscriber(
            stream=stream,
            subscription_id=subscription_id,
            client=client,
            reference=reference,
            callback=callback,
            decoder=decoder,
            offset_type=offset_type,
            offset=offset or 0,
        )
        return subscriber

    async def subscribe(
        self,
        stream: str,
        callback: CB[MT],
        *,
        decoder: Optional[Callable[[bytes], MT]] = None,
        offset: Optional[int] = None,
        offset_type: OffsetType = OffsetType.FIRST,
        initial_credit: int = 10,
        properties: Optional[dict[str, Any]] = None,
        subscriber_name: Optional[str] = None,
    ) -> str:
        async with self._lock:
            subscriber = await self._create_subscriber(
                stream=stream,
                subscriber_name=subscriber_name,
                callback=callback,
                decoder=decoder,
                offset_type=offset_type,
                offset=offset,
            )

        subscriber.client.add_handler(
            schema.Deliver,
            partial(self._on_deliver, subscriber=subscriber),
            name=subscriber.reference,
        )
        await subscriber.client.subscribe(
            stream=stream,
            subscription_id=subscriber.subscription_id,
            offset_spec=schema.OffsetSpec.from_params(offset_type, offset),
            initial_credit=initial_credit,
            properties=properties,
        )

        return subscriber.reference

    async def unsubscribe(self, subscriber_name: str) -> None:
        subscriber = self._subscribers[subscriber_name]
        subscriber.client.remove_handler(
            schema.Deliver,
            name=subscriber.reference,
        )
        await subscriber.client.unsubscribe(subscriber.subscription_id)
        del self._subscribers[subscriber_name]

    async def query_offset(self, stream: str, subscriber_name: str) -> int:
        return await self.default_client.query_offset(
            stream,
            subscriber_name,
        )

    async def store_offset(self, stream: str, subscriber_name: str, offset: int) -> None:
        await self.default_client.store_offset(
            stream=stream,
            reference=subscriber_name,
            offset=offset,
        )

    @staticmethod
    def _filter_messages(frame: schema.Deliver, subscriber: _Subscriber) -> Iterator[bytes]:
        if subscriber.offset_type is OffsetType.TIMESTAMP:
            if frame.timestamp < subscriber.offset:
                yield from ()
            else:
                yield from frame.get_messages()

        else:
            offset = frame.chunk_first_offset - 1
            if subscriber.offset_type is OffsetType.NEXT:
                offset -= 1

            for message in frame.get_messages():
                offset += 1
                if offset >= subscriber.offset:
                    yield message

            subscriber.offset = frame.chunk_first_offset + frame.num_entries

    async def _on_deliver(self, frame: schema.Deliver, subscriber: _Subscriber) -> None:
        if frame.subscription_id != subscriber.subscription_id:
            return

        await subscriber.client.credit(subscriber.subscription_id, 1)

        for message in self._filter_messages(frame, subscriber):
            maybe_coro = subscriber.callback(subscriber.decoder(message))
            if maybe_coro is not None:
                await maybe_coro

    async def create_stream(
        self,
        stream: str,
        arguments: Optional[dict[str, Any]] = None,
        exists_ok: bool = False,
    ) -> None:
        try:
            await self.default_client.create_stream(stream, arguments)
        except exceptions.StreamAlreadyExists:
            if not exists_ok:
                raise

    async def delete_stream(self, stream: str, missing_ok: bool = False) -> None:
        for subscriber in list(self._subscribers.values()):
            if subscriber.stream == stream:
                del self._subscribers[subscriber.reference]

        try:
            await self.default_client.delete_stream(stream)
        except exceptions.StreamDoesNotExist:
            if not missing_ok:
                raise

    async def stream_exists(self, stream: str) -> bool:
        return await self.default_client.stream_exists(stream)
