# Copyright 2023 VMware, Inc. All Rights Reserved.
# SPDX-License-Identifier: MIT

from __future__ import annotations

import asyncio
import ssl
from collections import defaultdict
from dataclasses import dataclass
from functools import partial
from typing import (
    Annotated,
    Any,
    Awaitable,
    Callable,
    Generic,
    NoReturn,
    Optional,
    TypeVar,
    Union,
)

from . import exceptions, schema, utils
from .amqp import _MessageProtocol
from .client import Addr, Client, ClientPool
from .compression import (
    CompressionHelper,
    CompressionType,
    ICompressionCodec,
)
from .utils import RawMessage

MessageT = TypeVar("MessageT", _MessageProtocol, bytes)
MT = TypeVar("MT")
CB = Annotated[Callable[[MT], Union[None, Awaitable[None]]], "Message callback type"]


@dataclass
class _Publisher:
    id: int
    reference: str
    stream: str
    sequence: utils.MonotonicSeq
    client: Client


@dataclass
class _MessageNotification(Generic[MessageT]):
    entry: MessageT | ICompressionCodec
    callback: Optional[CB[ConfirmationStatus]] = None
    publisher_name: Optional[str] = None


@dataclass
class ConfirmationStatus:
    message_id: int
    is_confirmed: bool = False
    response_code: int = 0


class Producer:
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
        default_batch_publishing_delay: float = 0.2,
        default_context_switch_value: int = 1000,
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
        self._publishers: dict[str, _Publisher] = {}
        self._waiting_for_confirm: dict[
            str, dict[asyncio.Future[None] | CB[ConfirmationStatus], set[int]]
        ] = defaultdict(dict)
        self._lock = asyncio.Lock()
        # dictionary [stream][list] of buffered messages to send asynchronously
        self._buffered_messages: dict[str, list] = defaultdict(list)
        self._buffered_messages_lock = asyncio.Lock()
        self.task: asyncio.Task[NoReturn] | None = None
        # Delay After sending the messages on _buffered_messages list
        self._default_batch_publishing_delay = default_batch_publishing_delay
        self._default_context_switch_counter = 0
        self._default_context_switch_value = default_context_switch_value
        self._connection_closed_handler = connection_closed_handler

    @property
    def default_client(self) -> Client:
        if self._default_client is None:
            raise ValueError("Producer is not started")
        return self._default_client

    async def __aenter__(self) -> Producer:
        await self.start()
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.close()

    async def start(self) -> None:
        self._default_client = await self._pool.get(connection_closed_handler=self._connection_closed_handler)

    async def close(self) -> None:
        # flush messages still in buffer
        if self.task is not None:

            for stream in self._buffered_messages:
                await self._publish_buffered_messages(stream)
            self.task.cancel()

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
            self._clients[stream] = await self._pool.get(
                Addr(leader.host, leader.port), self._connection_closed_handler
            )

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
        reference = publisher_name or f"{stream}_publisher_{publisher_id}"
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

    async def send_batch(
        self,
        stream: str,
        batch: list[MessageT],
        publisher_name: Optional[str] = None,
        on_publish_confirm: Optional[CB[ConfirmationStatus]] = None,
    ) -> list[int]:

        wrapped_batch = []
        for item in batch:
            wrapped_item = _MessageNotification(
                entry=item, callback=on_publish_confirm, publisher_name=publisher_name
            )
            wrapped_batch.append(wrapped_item)

        return await self._send_batch(stream, wrapped_batch, sync=False)

    async def _send_batch(
        self,
        stream: str,
        batch: list[_MessageNotification],
        sync: bool = True,
    ) -> list[int]:
        if len(batch) == 0:
            raise ValueError("Empty batch")

        messages = []
        publishing_ids = set()
        publishing_ids_callback: dict[CB[ConfirmationStatus], set[int]] = defaultdict(set)

        for item in batch:

            async with self._lock:
                publisher = await self._get_or_create_publisher(stream, publisher_name=item.publisher_name)

            if not isinstance(item.entry, ICompressionCodec):

                msg = RawMessage(item.entry) if isinstance(item.entry, bytes) else item.entry

                if msg.publishing_id is None:
                    msg.publishing_id = publisher.sequence.next()

                messages.append(
                    schema.Message(
                        publishing_id=msg.publishing_id,
                        data=bytes(msg),
                    )
                )

                if item.callback is not None:
                    publishing_ids_callback[item.callback].update([msg.publishing_id])

            else:
                compression_codec = item.entry
                if len(messages) > 0:
                    await publisher.client.send_frame(
                        schema.Publish(
                            publisher_id=publisher.id,
                            messages=messages,
                        ),
                    )
                    # publishing_ids.update([m.publishing_id for m in messages])
                    messages.clear()
                for _ in range(item.entry.messages_count()):
                    publishing_id = publisher.sequence.next()

                await publisher.client.send_frame(
                    schema.PublishSubBatching(
                        publisher_id=publisher.id,
                        number_of_root_messages=1,
                        publishing_id=publishing_id,
                        compress_type=0x80 | compression_codec.compression_type() << 4,
                        subbatching_message_count=compression_codec.messages_count(),
                        uncompressed_data_size=compression_codec.uncompressed_size(),
                        compressed_data_size=compression_codec.compressed_size(),
                        messages=compression_codec.data(),
                    ),
                )
                publishing_ids.update([publishing_id])

                if item.callback is not None:
                    publishing_ids_callback[item.callback].update([publishing_id])

        if len(messages) > 0:

            await publisher.client.send_frame(
                schema.Publish(
                    publisher_id=publisher.id,
                    messages=messages,
                ),
            )
            publishing_ids.update([m.publishing_id for m in messages])

        for callback in publishing_ids_callback:
            self._waiting_for_confirm[publisher.reference][callback] = publishing_ids_callback[
                callback
            ].copy()
        # this is just called in case of send_wait
        if sync:
            future: asyncio.Future[None] = asyncio.Future()
            self._waiting_for_confirm[publisher.reference][future] = publishing_ids.copy()
            await future

        return list(publishing_ids)

    async def send_wait(
        self,
        stream: str,
        message: MessageT,
        publisher_name: Optional[str] = None,
    ) -> int:

        wrapped_message: _MessageNotification = _MessageNotification(
            entry=message, callback=None, publisher_name=publisher_name
        )

        publishing_ids = await self._send_batch(
            stream,
            [wrapped_message],
            sync=True,
        )
        return publishing_ids[0]

    def _timer_completed(self, context):

        if not context.cancelled():
            if context.exception():
                raise context.exception()

        return 0

    async def send(
        self,
        stream: str,
        message: MessageT,
        publisher_name: Optional[str] = None,
        on_publish_confirm: Optional[CB[ConfirmationStatus]] = None,
    ):

        # start the background thread to send buffered messages
        if self.task is None:
            self.task = asyncio.create_task(self._timer())
            self.task.add_done_callback(self._timer_completed)

        wrapped_message = _MessageNotification(
            entry=message, callback=on_publish_confirm, publisher_name=publisher_name
        )
        async with self._buffered_messages_lock:
            self._buffered_messages[stream].append(wrapped_message)

        self._default_context_switch_counter += 1

        if self._default_context_switch_counter > self._default_context_switch_value:
            await asyncio.sleep(0)
            self._default_context_switch_counter = 0

    async def send_sub_entry(
        self,
        stream: str,
        sub_entry_messages: list[MessageT],
        compression_type: CompressionType = CompressionType.No,
        publisher_name: Optional[str] = None,
        on_publish_confirm: Optional[CB[ConfirmationStatus]] = None,
    ):

        if len(sub_entry_messages) == 0:
            raise ValueError("Empty batch")

        # start the background thread to send buffered messages
        if self.task is None:
            self.task = asyncio.create_task(self._timer())
            self.task.add_done_callback(self._timer_completed)

        compression_codec = CompressionHelper.compress(sub_entry_messages, compression_type)

        wrapped_message = _MessageNotification(
            entry=compression_codec, callback=on_publish_confirm, publisher_name=publisher_name
        )
        async with self._buffered_messages_lock:
            self._buffered_messages[stream].append(wrapped_message)

        await asyncio.sleep(0)

    # After the timeout send the messages in _buffered_messages in batches
    async def _timer(self):

        while True:
            await asyncio.sleep(self._default_batch_publishing_delay)
            for stream in self._buffered_messages:
                await self._publish_buffered_messages(stream)

    async def _publish_buffered_messages(self, stream: str) -> None:

        async with self._buffered_messages_lock:
            if len(self._buffered_messages[stream]):
                await self._send_batch(stream, self._buffered_messages[stream], sync=False)
                self._buffered_messages[stream].clear()

    def _on_publish_confirm(self, frame: schema.PublishConfirm, publisher: _Publisher) -> None:

        if frame.publisher_id != publisher.id:
            return

        waiting = self._waiting_for_confirm[publisher.reference]
        for confirmation in list(waiting):
            ids = waiting[confirmation]
            ids_to_call = ids.intersection(frame.publishing_ids)

            for id in ids_to_call:
                if not isinstance(confirmation, asyncio.Future):
                    confirmation_status: ConfirmationStatus = ConfirmationStatus(id, True)
                    confirmation(confirmation_status)
            ids.difference_update(frame.publishing_ids)
            if not ids:
                del waiting[confirmation]
                if isinstance(confirmation, asyncio.Future):
                    confirmation.set_result(None)

    def _on_publish_error(self, frame: schema.PublishError, publisher: _Publisher) -> None:

        if frame.publisher_id != publisher.id:
            return

        waiting = self._waiting_for_confirm[publisher.reference]
        for error in frame.errors:
            exc = exceptions.ServerError.from_code(error.response_code)
            for confirmation in list(waiting):
                ids = waiting[confirmation]
                if error.publishing_id in ids:
                    if not isinstance(confirmation, asyncio.Future):
                        confirmation_status = ConfirmationStatus(
                            error.publishing_id, False, error.response_code
                        )
                        confirmation(confirmation_status)
                        ids.remove(error.publishing_id)

                if not ids:
                    del waiting[confirmation]
                    if isinstance(confirmation, asyncio.Future):
                        confirmation.set_exception(exc)

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
        if stream in self._publishers:
            publisher = self._publishers[stream]
            await publisher.client.delete_publisher(publisher.id)
            publisher.client.remove_handler(schema.PublishConfirm, publisher.reference)
            publisher.client.remove_handler(schema.PublishError, publisher.reference)
            del self._publishers[stream]

        try:
            await self.default_client.delete_stream(stream)
        except exceptions.StreamDoesNotExist:
            if not missing_ok:
                raise

    async def stream_exists(self, stream: str) -> bool:
        return await self.default_client.stream_exists(stream)
