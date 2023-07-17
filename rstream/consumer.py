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
from .amqp import AMQPMessage
from .client import Addr, Client, ClientPool
from .constants import (
    ConsumerOffsetSpecification,
    OffsetType,
)
from .schema import OffsetSpecification

MT = TypeVar("MT")
CB = Annotated[Callable[[MT, Any], Union[None, Awaitable[None]]], "Message callback type"]
CB_CONN = Annotated[Callable[[MT], Union[None, Awaitable[None]]], "Message callback type"]


@dataclass
class MessageContext:
    consumer: Consumer
    subscriber_name: str
    offset: int
    timestamp: int


@dataclass
class EventContext:
    consumer: Consumer
    subscriber_name: str
    reference: str


@dataclass
class _Subscriber:
    stream: str
    subscription_id: int
    reference: str
    client: Client
    callback: Callable[[AMQPMessage, MessageContext], Union[None, Awaitable[None]]]
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
        load_balancer_mode: bool = False,
        max_retries: int = 20,
        connection_closed_handler: Optional[CB_CONN[Exception]] = None,
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
            load_balancer_mode=load_balancer_mode,
            max_retries=max_retries,
        )

        self._default_client: Optional[Client] = None
        self._clients: dict[str, Client] = {}
        self._subscribers: dict[str, _Subscriber] = {}
        self._stop_event = asyncio.Event()
        self._lock = asyncio.Lock()
        self._connection_closed_handler = connection_closed_handler

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
        self._default_client = await self._pool.get(connection_closed_handler=self._connection_closed_handler)

    def stop(self) -> None:
        self._stop_event.set()

    async def close(self) -> None:
        self.stop()

        for subscriber in list(self._subscribers.values()):
            await self.unsubscribe(subscriber.reference)
            # await self.store_offset(subscriber.stream, subscriber.reference, subscriber.offset)

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
            self._clients[stream] = await self._pool.get(
                addr=Addr(broker.host, broker.port), connection_closed_handler=self._connection_closed_handler
            )

        return self._clients[stream]

    async def _create_subscriber(
        self,
        stream: str,
        subscriber_name: Optional[str],
        callback: Callable[[AMQPMessage, MessageContext], Union[None, Awaitable[None]]],
        decoder: Optional[Callable[[bytes], Any]],
        offset_type: OffsetType,
        offset: Optional[int],
    ) -> _Subscriber:
        client = await self._get_or_create_client(stream)

        # We can have multiple subscribers sharing same connection, so their ids must be distinct
        subscription_id = len([s for s in self._subscribers.values() if s.client is client]) + 1
        reference = subscriber_name or f"{stream}_subscriber_{subscription_id}"
        decoder = decoder or (lambda x: x)

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
        callback: Callable[[AMQPMessage, MessageContext], Union[None, Awaitable[None]]],
        *,
        decoder: Optional[Callable[[bytes], MT]] = None,
        offset_specification: Optional[ConsumerOffsetSpecification] = None,
        initial_credit: int = 10,
        properties: Optional[dict[str, Any]] = None,
        subscriber_name: Optional[str] = None,
        consumer_update_listener: Optional[Callable[[bool, EventContext], Awaitable[Any]]] = None,
    ) -> str:

        if offset_specification is None:
            offset_specification = ConsumerOffsetSpecification(OffsetType.FIRST, None)

        async with self._lock:
            subscriber = await self._create_subscriber(
                stream=stream,
                subscriber_name=subscriber_name,
                callback=callback,
                decoder=decoder,
                offset_type=offset_specification.offset_type,
                offset=offset_specification.offset,
            )

        subscriber.client.add_handler(
            schema.Deliver,
            partial(self._on_deliver, subscriber=subscriber),
            name=subscriber.reference,
        )

        # to handle single-active-consumer
        if properties is not None:
            if "single-active-consumer" in properties:
                subscriber.client.add_handler(
                    schema.ConsumerUpdateResponse,
                    partial(
                        self._on_consumer_update_query_response,
                        subscriber=subscriber,
                        reference=properties["name"],
                        consumer_update_listener=consumer_update_listener,
                    ),
                    name=subscriber.reference,
                )

        await subscriber.client.subscribe(
            stream=stream,
            subscription_id=subscriber.subscription_id,
            offset_spec=schema.OffsetSpec.from_params(
                offset_specification.offset_type, offset_specification.offset
            ),
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
        if subscriber_name == "":
            raise ValueError("subscriber_name must not be an empty string")

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
        min_deliverable_offset = -1
        if subscriber.offset_type is OffsetType.OFFSET:
            min_deliverable_offset = subscriber.offset

        offset = frame.chunk_first_offset - 1

        for message in frame.get_messages():
            offset += 1
            if offset < min_deliverable_offset:
                continue

            yield message

        subscriber.offset = frame.chunk_first_offset + frame.num_entries

    async def _on_deliver(self, frame: schema.Deliver, subscriber: _Subscriber) -> None:

        if frame.subscription_id != subscriber.subscription_id:
            return

        await subscriber.client.credit(subscriber.subscription_id, 1)
        offset = frame.chunk_first_offset

        for index, message in enumerate(self._filter_messages(frame, subscriber)):
            offset = offset + index
            message_context = MessageContext(self, subscriber.reference, offset, frame.timestamp)

            maybe_coro = subscriber.callback(subscriber.decoder(message), message_context)
            if maybe_coro is not None:
                await maybe_coro

    async def _on_consumer_update_query_response(
        self,
        frame: schema.ConsumerUpdateResponse,
        subscriber: _Subscriber,
        reference: str,
        consumer_update_listener: Optional[Callable[[bool, EventContext], Awaitable[Any]]] = None,
    ) -> None:

        # event the consumer is not active, we need to send a ConsumerUpdateResponse
        # by protocol definition. the offsetType can't be null so we use OffsetTypeNext as default
        if consumer_update_listener is None:
            offset_specification = OffsetSpecification(OffsetType.NEXT, 0)
            await subscriber.client.consumer_update(frame.correlation_id, offset_specification)

        else:
            is_active = bool(frame.active)
            event_context = EventContext(self, subscriber.reference, reference)
            offset_specification = await consumer_update_listener(is_active, event_context)
            await subscriber.client.consumer_update(frame.correlation_id, offset_specification)

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

    async def stream(self, subscriber_name) -> str:

        return self._subscribers[subscriber_name].stream

    def get_stream(self, subscriber_name) -> str:

        return self._subscribers[subscriber_name].stream
