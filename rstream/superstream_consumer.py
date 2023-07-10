# Copyright 2023 VMware, Inc. All Rights Reserved.
# SPDX-License-Identifier: MIT

from __future__ import annotations

import asyncio
import random
import ssl
from collections import defaultdict
from typing import (
    Annotated,
    Any,
    Awaitable,
    Callable,
    Optional,
    TypeVar,
    Union,
)

from .amqp import AMQPMessage
from .client import Addr, Client, ClientPool
from .constants import (
    ConsumerOffsetSpecification,
    OffsetType,
)
from .consumer import Consumer, EventContext, MessageContext
from .superstream import DefaultSuperstreamMetadata

MT = TypeVar("MT")
CB = Annotated[Callable[[MT], Union[None, Awaitable[None]]], "Message callback type"]


class SuperStreamConsumer:
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
        super_stream: str,
        connection_closed_handler: Optional[CB[Exception]] = None,
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
        self._lock = asyncio.Lock()
        self.super_stream = super_stream
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.vhost = vhost
        self.ssl_context = ssl_context
        self.frame_max = frame_max
        self.heartbeat = heartbeat
        self.load_balancer_mode = load_balancer_mode
        self.max_retries = max_retries
        self._consumers: dict[str, Consumer] = {}
        self._stop_event = asyncio.Event()
        self._subscribers: dict[str, str] = defaultdict(str)
        self._connection_closed_handler = connection_closed_handler

    @property
    def default_client(self) -> Client:
        if self._default_client is None:
            raise ValueError("Consumer is not started")
        return self._default_client

    async def __aenter__(self) -> SuperStreamConsumer:
        await self.start()
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.close()

    async def start(self) -> None:
        self._default_client = await self._pool.get(connection_closed_handler=self._connection_closed_handler)

    def stop(self) -> None:
        self._stop_event.set()

    async def close(self) -> None:
        for partition in self._consumers:
            consumer = self._consumers[partition]
            await consumer.close()

        self.stop()
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
                Addr(broker.host, broker.port), connection_closed_handler=self._connection_closed_handler
            )

        return self._clients[stream]

    async def subscribe(
        self,
        callback: Callable[[AMQPMessage, MessageContext], Union[None, Awaitable[None]]],
        *,
        decoder: Optional[Callable[[bytes], MT]] = None,
        offset_specification: Optional[ConsumerOffsetSpecification] = None,
        initial_credit: int = 10,
        properties: Optional[dict[str, Any]] = None,
        subscriber_name: Optional[str] = None,
        consumer_update_listener: Optional[Callable[[bool, EventContext], Awaitable[Any]]] = None,
    ):

        if offset_specification is None:
            offset_specification = ConsumerOffsetSpecification(OffsetType.FIRST, None)

        self._super_stream_metadata = DefaultSuperstreamMetadata(self.super_stream, self.default_client)
        partitions = await self._super_stream_metadata.partitions()

        for partition in partitions:
            if partition not in self._consumers.keys():
                consumer = await self._create_consumer()
                self._consumers[partition] = consumer

            consumer_partition: Optional[Consumer] = self._consumers.get(partition)
            if consumer_partition is None:
                return

            subscriber = await consumer_partition.subscribe(
                stream=partition,
                callback=callback,
                decoder=decoder,
                offset_specification=offset_specification,
                initial_credit=initial_credit,
                properties=properties,
                subscriber_name=subscriber_name,
                consumer_update_listener=consumer_update_listener,
            )
            self._subscribers[partition] = subscriber

    async def _create_consumer(self) -> Consumer:
        consumer = Consumer(
            host=self.host,
            port=self.port,
            vhost=self.vhost,
            username=self.username,
            password=self.password,
            ssl_context=self.ssl_context,
            frame_max=self.frame_max,
            heartbeat=self.heartbeat,
            load_balancer_mode=False,
            max_retries=self.max_retries,
        )

        await consumer.start()

        return consumer

    async def unsubscribe(self) -> None:

        partitions = await self._super_stream_metadata.partitions()
        for partition in partitions:
            if self._consumers[partition] is None:
                self._consumers[partition] = await self._create_consumer()

            consumer = self._consumers[partition]
            await consumer.unsubscribe(self._subscribers[partition])
