# Copyright 2023 VMware, Inc. All Rights Reserved.
# SPDX-License-Identifier: MIT

from __future__ import annotations

import asyncio
import inspect
import logging
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
from .constants import SlasMechanism
from .utils import DisconnectionErrorInfo, RawMessage

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


logger = logging.getLogger(__name__)


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
        connection_name: str = None,
        connection_closed_handler: Optional[CB[DisconnectionErrorInfo]] = None,
        sasl_configuration_mechanism: SlasMechanism = SlasMechanism.MechanismPlain,
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
            sasl_configuration_mechanism=sasl_configuration_mechanism,
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
        self._sasl_configuration_mechanism = sasl_configuration_mechanism
        self._close_called = False
        self._connection_name = connection_name

        if self._connection_name is None:
            self._connection_name = "rstream-producer"

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
        self._close_called = False
        self._default_client = await self._pool.get(
            connection_closed_handler=self._connection_closed_handler,
            connection_name=self._connection_name,
            sasl_configuration_mechanism=self._sasl_configuration_mechanism,
        )

    async def close(self) -> None:

        # check if we are in a server disconnection situation:
        # in this case we need avoid other send
        # otherwise if is a normal close() we need to send the last item in batch
        for publisher in self._publishers.values():
            if publisher.client.is_connection_alive() is False:
                self._close_called = True
                # just in this special case give time to all the tasks to complete
                await asyncio.sleep(0.2)
                break

        # flush messages still in buffer
        if self._default_client is None:
            return

        if self.default_client.is_connection_alive():
            if self.task is not None:
                for stream in self._buffered_messages:
                    await self._publish_buffered_messages(stream)
                self.task.cancel()

        self._close_called = True

        for publisher in list(self._publishers.values()):
            if publisher.client.is_connection_alive():
                try:
                    await asyncio.wait_for(publisher.client.delete_publisher(publisher.id), 5)
                except asyncio.TimeoutError:
                    logger.debug("timeout when closing producer and deleting publisher")
                except BaseException as exc:
                    logger.debug("exception in delete_publisher in Producer.close:", exc)
            publisher.client.remove_handler(schema.PublishConfirm, publisher.reference)
            publisher.client.remove_handler(schema.PublishError, publisher.reference)

        self._publishers.clear()

        await self._pool.close()
        self._clients.clear()
        self._waiting_for_confirm.clear()
        self._default_client = None

    async def _get_or_create_client(self, stream: str) -> Client:
        if stream not in self._clients:
            if self._default_client is None:
                self._default_client = await self._pool.get(
                    connection_closed_handler=self._connection_closed_handler,
                    connection_name=self._connection_name,
                )
            leader, _ = await self.default_client.query_leader_and_replicas(stream)
            self._clients[stream] = await self._pool.get(
                connection_name=self._connection_name,
                addr=Addr(leader.host, leader.port),
                connection_closed_handler=self._connection_closed_handler,
                stream=stream,
                sasl_configuration_mechanism=self._sasl_configuration_mechanism,
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

        await self.check_connection()

        if len(batch) == 0:
            raise ValueError("Empty batch")

        if self._close_called:
            return []

        return await self._send_batch(
            stream=stream, batch=batch, publisher_name=publisher_name, callback=on_publish_confirm, sync=False
        )

    # used by send_batch and send_wait
    async def _send_batch(
        self,
        stream: str,
        batch: list[MessageT],
        publisher_name: Optional[str] = None,
        callback: Optional[CB[ConfirmationStatus]] = None,
        sync: bool = True,
        timeout: Optional[int] = None,
    ) -> list[int]:

        messages = []
        publishing_ids = set()

        async with self._lock:
            publisher = await self._get_or_create_publisher(stream, publisher_name=publisher_name)

        for item in batch:

            msg = RawMessage(item) if isinstance(item, bytes) else item

            if msg.publishing_id is None:
                msg.publishing_id = publisher.sequence.next()

            messages.append(
                schema.Message(
                    publishing_id=msg.publishing_id,
                    data=bytes(msg),
                )
            )

        if len(messages) > 0:
            await publisher.client.send_frame(
                schema.Publish(
                    publisher_id=publisher.id,
                    messages=messages,
                ),
            )
            publishing_ids.update([m.publishing_id for m in messages])

        if not sync:
            if callback is not None:
                if callback not in self._waiting_for_confirm[publisher.reference]:
                    self._waiting_for_confirm[publisher.reference][callback] = set()

                self._waiting_for_confirm[publisher.reference][callback].update(publishing_ids)

        # this is just called in case of send_wait
        else:
            future: asyncio.Future[None] = asyncio.Future()
            self._waiting_for_confirm[publisher.reference][future] = publishing_ids.copy()
            await asyncio.wait_for(future, timeout)

        return list(publishing_ids)

    # used by send, send_sub_batch (with compression)
    async def _send_batch_async(
        self,
        stream: str,
        batch: list[_MessageNotification],
    ) -> list[int]:
        if len(batch) == 0:
            raise ValueError("Empty batch")

        await self.check_connection()
        if self._close_called:
            return []

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
                    publishing_ids_callback[item.callback].add(msg.publishing_id)

            else:
                compression_codec = item.entry
                if len(messages) > 0:
                    await publisher.client.send_publish_frame(
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
                    publishing_ids_callback[item.callback].add(publishing_id)

        if len(messages) > 0:
            await publisher.client.send_publish_frame(
                schema.Publish(
                    publisher_id=publisher.id,
                    messages=messages,
                ),
            )
            publishing_ids.update([m.publishing_id for m in messages])

        for callback in publishing_ids_callback:

            if callback not in self._waiting_for_confirm[publisher.reference]:
                self._waiting_for_confirm[publisher.reference][callback] = set()

            self._waiting_for_confirm[publisher.reference][callback].update(publishing_ids_callback[callback])

        return list(publishing_ids)

    async def send_wait(
        self,
        stream: str,
        message: MessageT,
        publisher_name: Optional[str] = None,
        timeout: Optional[int] = 5,
    ) -> int:

        await self.check_connection()

        publishing_ids = await self._send_batch(
            stream,
            [message],
            publisher_name=publisher_name,
            sync=True,
            timeout=timeout,
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

        await self.check_connection()

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

        while not self._close_called:
            await asyncio.sleep(self._default_batch_publishing_delay)
            for stream in self._buffered_messages:
                try:
                    await self._publish_buffered_messages(stream)
                except BaseException as exc:
                    logger.debug("producer _timer exception: ", {exc})

    async def _publish_buffered_messages(self, stream: str) -> None:

        if stream in self._clients:
            if self._clients[stream].is_connection_alive() is False:
                return

        async with self._buffered_messages_lock:
            if len(self._buffered_messages[stream]):
                await self._send_batch_async(stream, self._buffered_messages[stream])
                self._buffered_messages[stream].clear()

    async def _on_publish_confirm(self, frame: schema.PublishConfirm, publisher: _Publisher) -> None:

        if frame.publisher_id != publisher.id:
            return

        waiting = self._waiting_for_confirm[publisher.reference]
        for confirmation in list(waiting):
            ids = waiting[confirmation]
            ids_to_call = ids.intersection(set(frame.publishing_ids))

            for id in ids_to_call:
                if not isinstance(confirmation, asyncio.Future):
                    confirmation_status: ConfirmationStatus = ConfirmationStatus(id, True)
                    result = confirmation(confirmation_status)
                    if result is not None and hasattr(result, "__await__"):
                        await result
            ids.difference_update(frame.publishing_ids)
            if not ids:
                del waiting[confirmation]
                if isinstance(confirmation, asyncio.Future):
                    confirmation.set_result(None)

    async def _on_publish_error(self, frame: schema.PublishError, publisher: _Publisher) -> None:

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
                        result = confirmation(confirmation_status)
                        if result is not None and inspect.isawaitable(result):
                            await result
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

    async def check_connection(self):

        for publisher in self._publishers.values():
            if publisher.client.is_connection_alive() is False:
                raise Exception("connection Closed")

    async def reconnect_stream(self, stream: str) -> None:

        if stream in self._clients:
            del self._clients[stream]
        if stream in self._publishers:
            async with self._lock:
                await self._publishers[stream].client.close()
                del self._publishers[stream]

        if self._default_client is not None:
            if self._default_client.is_connection_alive() is False:
                await self._default_client.close()
                self._default_client = None

        async with self._lock:
            await self._get_or_create_publisher(stream)
