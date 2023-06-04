# Copyright 2023 VMware, Inc. All Rights Reserved.
# SPDX-License-Identifier: MIT

import abc
import ssl
from enum import Enum
from typing import (
    Annotated,
    Any,
    Awaitable,
    Callable,
    Optional,
    TypeVar,
    Union,
)

import mmh3

from .amqp import _MessageProtocol
from .client import Client, ClientPool
from .producer import ConfirmationStatus, Producer

MT = TypeVar("MT")
CB = Annotated[Callable[[MT], Union[None, Awaitable[None]]], "Message callback type"]

MessageT = TypeVar("MessageT", _MessageProtocol, bytes)


class RouteType(Enum):
    Hash = 0
    Key = 1


class SuperStreamProducer:
    def __init__(
        self,
        host: str,
        port: int = 5552,
        *,
        ssl_context: Optional[ssl.SSLContext] = None,
        vhost: str = "/",
        username: str,
        password: str,
        super_stream: str,
        routing_extractor: CB[Any],
        routing: RouteType = RouteType.Hash,
        frame_max: int = 1 * 1024 * 1024,
        heartbeat: int = 60,
        load_balancer_mode: bool = False,
        max_retries: int = 20,
        default_batch_publishing_delay: float = 0.2,
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
        self.host = host
        self.port = port
        self.vhost = vhost
        self.username = username
        self.password = password
        self.ssl_context = ssl_context
        self.super_stream = super_stream
        self.routing = routing
        self.routing_extractor: CB[Any] = routing_extractor
        self.frame_max = frame_max
        self.heartbeat = heartbeat
        self.load_balancer_mode = load_balancer_mode
        self.max_retries = max_retries
        self.default_batch_publishing_delay = default_batch_publishing_delay
        self._default_client: Optional[Client] = None
        self._producer = None
        self._routing_strategy: RoutingStrategy

    async def _get_producer(self) -> Producer:
        if self._producer is None:
            producer = Producer(
                host=self.host,
                port=self.port,
                vhost=self.vhost,
                username=self.username,
                password=self.password,
                ssl_context=self.ssl_context,
                frame_max=self.frame_max,
                heartbeat=self.heartbeat,
                load_balancer_mode=self.load_balancer_mode,
                default_batch_publishing_delay=self.default_batch_publishing_delay,
            )
            await producer.start()
            self._producer = producer
        return self._producer

    async def send(
        self,
        message: MessageT,
        on_publish_confirm: Optional[CB[ConfirmationStatus]] = None,
    ) -> None:

        streams = await self._routing_strategy.route(message, self.super_stream_metadata)
        self._producer = await self._get_producer()

        for stream in streams:
            print("stream is: " + stream)
            await self._producer.send(stream=stream, message=message, on_publish_confirm=on_publish_confirm)

    @property
    def default_client(self) -> Client:
        if self._default_client is None:
            raise ValueError("Producer is not started")
        return self._default_client

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.close()

    async def start(self) -> None:
        self._default_client = await self._pool.get()
        self.super_stream_metadata = DefaultSuperstreamMetadata(self.super_stream, self._default_client)
        if self.routing == RouteType.Hash:
            self._routing_strategy = HashRoutingMurmurStrategy(self.routing_extractor)
        else:
            self._routing_strategy = RoutingKeyRoutingStrategy(self.routing_extractor)

    async def close(self) -> None:
        await self._pool.close()
        await self._producer.close()
        self._default_client = None


class Metadata(abc.ABC):
    async def partitions(self) -> list[str]:
        pass

    async def routes(self, routing_key: str) -> list[str]:
        pass


class DefaultSuperstreamMetadata(Metadata):
    def __init__(self, super_stream: str, client: Client):
        self.super_stream = super_stream
        self.client = client

    async def partitions(self) -> list[str]:
        self._partitions = await self.client.partitions(self.super_stream)
        return self._partitions

    async def routes(self, routing_key: str) -> list[str]:
        self.route = await self.client.route(routing_key, self.super_stream)
        return self.route


class RoutingStrategy(abc.ABC):
    async def route(self, message: MessageT, metadata: Metadata) -> list[str]:
        pass


class RoutingKeyRoutingStrategy(RoutingStrategy):
    def __init__(self, routingKeyExtractor: CB[Any]):
        self.routingKeyExtractor = routingKeyExtractor

    async def route(self, message: MessageT, metadata: Metadata) -> list[str]:
        key = self.routingKeyExtractor(message)
        return await metadata.routes(str(key))


class HashRoutingMurmurStrategy(RoutingStrategy):
    def __init__(self, routingKeyExtractor: CB[Any]):
        self.routingKeyExtractor = routingKeyExtractor

    async def route(self, message: MessageT, metadata: Metadata) -> list[str]:
        streams = []
        key = self.routingKeyExtractor(message)
        hash = mmh3.hash_bytes(bytes(key, "UTF-16"), 104729)
        partitions = len(await metadata.partitions())

        route = int.from_bytes(hash, "little", signed=False) % partitions
        streams = await metadata.partitions()
        stream = streams[route]
        streams.append(stream)

        return streams
