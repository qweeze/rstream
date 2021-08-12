from __future__ import annotations

import asyncio
from collections import defaultdict
from dataclasses import dataclass
from functools import partial
from typing import Any, Optional

from . import exceptions, schema, utils
from .client import Client, ClientPool


@dataclass
class _Publisher:
    id: int
    reference: str
    stream: str
    sequence: utils.MonotonicSeq
    client: Client


class Producer:
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
        self._publishers: dict[str, _Publisher] = {}
        self._waiting_for_confirm: dict[
            str,
            dict[asyncio.Future[None], set[int]]
        ] = defaultdict(dict)

    @property
    def default_client(self) -> Client:
        if self._default_client is None:
            raise ValueError('Producer is not started')
        return self._default_client

    async def __aenter__(self) -> Producer:
        await self.start()
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.close()

    async def start(self) -> None:
        self._default_client = await self._pool.get()

    async def close(self) -> None:
        for publisher in self._publishers.values():
            await publisher.client.delete_publisher(publisher.id)
            publisher.client.remove_handler(schema.PublishConfirm, publisher.reference)
            publisher.client.remove_handler(schema.PublishError, publisher.reference)

        self._publishers.clear()

        await self._pool.close()
        self._clients.clear()
        self._waiting_for_confirm.clear()
        self._default_client = None

    async def _get_or_create_client(self, stream: str) -> Client:
        if stream not in self._clients:
            leader, _ = await self.default_client.query_leader_and_replicas(stream)
            self._clients[stream] = await self._pool.get((leader.host, leader.port))

        return self._clients[stream]

    async def _get_or_create_publisher(
        self,
        stream: str,
        publisher_name: Optional[str] = None,
    ) -> _Publisher:
        if stream in self._publishers:
            publisher = self._publishers[stream]
            if publisher_name is not None:
                assert publisher.reference == publisher_name

            return publisher

        client = await self._get_or_create_client(stream)

        # We can have multiple publishers sharing same connection, so their ids must be distinct
        publisher_id = len([p for p in self._publishers.values() if p.client is client]) + 1
        reference = publisher_name or f'{stream}_publisher_{publisher_id}'
        publisher = self._publishers[stream] = _Publisher(
            id=publisher_id,
            stream=stream,
            reference=reference,
            sequence=utils.MonotonicSeq(),
            client=client,
        )
        await client.declare_publisher(
            stream=stream,
            reference=publisher.reference,
            publisher_id=publisher.id,
        )
        sequence = await client.query_publisher_sequence(
            stream=stream,
            reference=reference,
        )
        publisher.sequence.set(sequence + 1)

        client.add_handler(
            schema.PublishConfirm,
            partial(self._on_publish_confirm, publisher=publisher),
            name=publisher.reference,
        )
        client.add_handler(
            schema.PublishError,
            partial(self._on_publish_error, publisher=publisher),
            name=publisher.reference,
        )
        return publisher

    async def publish_batch(
        self,
        stream: str,
        batch: list[bytes],
        sync: bool = True,
        publisher_name: Optional[str] = None,
    ) -> list[int]:
        publisher = await self._get_or_create_publisher(stream, publisher_name)

        messages = [
            schema.Message(
                publishing_id=publisher.sequence.next(),
                data=data,
            ) for data in batch
        ]
        await publisher.client.send_frame(
            schema.Publish(
                publisher_id=publisher.id,
                messages=messages,
            ),
        )
        publishing_ids = [m.publishing_id for m in messages]

        if sync:
            future: asyncio.Future[None] = asyncio.Future()
            self._waiting_for_confirm[publisher.reference][future] = set(publishing_ids)
            await future

        return publishing_ids

    async def publish(
        self,
        stream: str,
        data: bytes,
        sync: bool = True,
        publisher_name: Optional[str] = None,
    ) -> int:
        publishing_ids = await self.publish_batch(
            stream,
            [data],
            sync=sync,
            publisher_name=publisher_name,
        )
        return publishing_ids[0]

    def _on_publish_confirm(self, frame: schema.PublishConfirm, publisher: _Publisher) -> None:
        if frame.publisher_id != publisher.id:
            return

        waiting = self._waiting_for_confirm[publisher.reference]
        for fut in list(waiting):
            ids = waiting[fut]
            ids.difference_update(frame.publishing_ids)
            if not ids:
                fut.set_result(None)
                del waiting[fut]

    def _on_publish_error(self, frame: schema.PublishError, publisher: _Publisher) -> None:
        if frame.publisher_id != publisher.id:
            return

        waiting = self._waiting_for_confirm[publisher.reference]
        for error in frame.errors:
            exc = exceptions.ServerError.from_code(error.response_code)
            for fut in list(waiting):
                ids = waiting[fut]
                if error.publishing_id in ids:
                    fut.set_exception(exc)
                    del waiting[fut]

    async def create_stream(self, stream: str, arguments: Optional[dict[str, Any]] = None) -> None:
        await self.default_client.create_stream(stream, arguments)

    async def delete_stream(self, stream: str) -> None:
        if stream in self._publishers:
            publisher = self._publishers[stream]
            await publisher.client.delete_publisher(publisher.id)
            publisher.client.remove_handler(schema.PublishConfirm, publisher.reference)
            publisher.client.remove_handler(schema.PublishError, publisher.reference)
            del self._publishers[stream]

        await self.default_client.delete_stream(stream)

    async def stream_exists(self, stream: str) -> bool:
        return await self.default_client.stream_exists(stream)
