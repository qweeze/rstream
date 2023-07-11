# Copyright 2023 VMware, Inc. All Rights Reserved.
# SPDX-License-Identifier: MIT

from __future__ import annotations

import asyncio
import logging
import socket
import ssl
import time
from collections import defaultdict
from contextlib import suppress
from typing import (
    Annotated,
    Any,
    Awaitable,
    Callable,
    NamedTuple,
    Optional,
    Type,
    TypeVar,
    Union,
)

from . import (
    __license__,
    __version__,
    constants,
    exceptions,
    schema,
    utils,
)
from .connection import Connection, ConnectionClosed
from .schema import OffsetSpecification

FT = TypeVar("FT", bound=schema.Frame)
HT = Annotated[
    Callable[[FT], Union[None, Awaitable[None]]],
    "Frame handler type",
]

DEFAULT_REQUEST_TIMEOUT = 10

MT = TypeVar("MT")
CB = Annotated[Callable[[MT], Union[None, Awaitable[None]]], "Message callback type"]

logger = logging.getLogger(__name__)


class BrokerResolutionMaxRetryError(Exception):
    pass


class Addr(NamedTuple):
    host: str
    port: int


class BaseClient:
    def __init__(
        self,
        host: str,
        port: int,
        *,
        ssl_context: Optional[ssl.SSLContext] = None,
        frame_max: int,
        heartbeat: int,
        connection_closed_handler: Optional[CB[Exception]] = None,
    ):
        self.host = host
        self.port = port
        self._ssl_context = ssl_context

        self._frame_max = frame_max
        self._heartbeat = heartbeat

        self._conn: Optional[Connection] = None

        self.server_properties: Optional[dict[str, str]] = None
        self._client_properties = {
            "product": "RabbitMQ Stream",
            "platform": "Python",
            "version": __version__,
            "license": __license__ or "",
        }

        self._corr_id_seq = utils.MonotonicSeq()
        self._waiters: dict[
            tuple[constants.Key, Optional[int]], set[asyncio.Future[schema.Frame]]
        ] = defaultdict(set)

        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._handlers: dict[Type[schema.Frame], dict[str, HT[Any]]] = defaultdict(dict)

        self._last_heartbeat: float = 0
        self._connection_closed_handler = connection_closed_handler

    def start_task(self, name: str, coro: Awaitable[None]) -> None:
        assert name not in self._tasks
        task = self._tasks[name] = asyncio.create_task(coro)

        def on_task_done(task: asyncio.Task[Any]) -> None:
            if not task.cancelled():
                task.result()

        task.add_done_callback(on_task_done)
        logger.debug("Started task %s", name)

    async def stop_task(self, name: str) -> None:
        logger.debug("Stopping task %s", name)
        task = self._tasks.pop(name, None)
        if task is not None:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

    def add_handler(
        self,
        frame_cls: Type[FT],
        handler: HT[FT],
        name: Optional[str] = None,
    ) -> None:
        if name is None:
            name = handler.__name__
        self._handlers[frame_cls][name] = handler

    def remove_handler(self, frame_cls: Type[FT], name: Optional[str] = None) -> None:
        if name is not None:
            del self._handlers[frame_cls][name]
        else:
            self._handlers[frame_cls].clear()

    async def send_frame(self, frame: schema.Frame) -> None:
        logger.debug("Sending frame: %s", frame)
        assert self._conn
        try:
            await self._conn.write_frame(frame)
        except socket.error as e:
            if self._connection_closed_handler is None:
                print("TCP connection closed")
            else:
                self._connection_closed_handler(e)

    def wait_frame(
        self,
        frame_cls: Type[FT],
        corr_id: Optional[int] = None,
        timeout: Optional[int] = None,
    ) -> Awaitable[FT]:
        if timeout is None:
            timeout = DEFAULT_REQUEST_TIMEOUT

        fut: asyncio.Future[schema.Frame] = asyncio.Future()
        _key = frame_cls.key, corr_id
        self._waiters[_key].add(fut)
        fut.add_done_callback(self._waiters[_key].discard)
        return utils.TimeoutWrapper(fut, timeout)

    async def sync_request(self, frame: schema.Frame, resp_schema: Type[FT], raise_exception=True) -> FT:
        waiter = self.wait_frame(resp_schema, frame.corr_id)
        await self.send_frame(frame)
        resp = await waiter
        resp.check_response_code(raise_exception=raise_exception)
        return resp

    async def start(self) -> None:
        logger.info("Starting client %s:%s", self.host, self.port)
        assert self._conn is None
        self._conn = Connection(self.host, self.port, self._ssl_context)
        await self._conn.open()
        self.start_task("listener", self._listener())
        self.add_handler(schema.Heartbeat, self._on_heartbeat)
        self.add_handler(schema.Close, self._on_close)

    async def _listener(self) -> None:
        assert self._conn
        while True:
            try:
                frame = await self._conn.read_frame()
            except ConnectionClosed as e:
                if self._connection_closed_handler is not None:
                    self._connection_closed_handler(e)
                else:
                    print("TCP connection closed")
                break
            except socket.error as e:
                if self._connection_closed_handler is not None:
                    self._connection_closed_handler(e)
                else:
                    print("TCP connection closed")
                break

            logger.debug("Received frame: %s", frame)
            _key = frame.key, frame.corr_id

            while self._waiters[_key]:
                fut = self._waiters[_key].pop()
                fut.set_result(frame)

            for _, handler in self._handlers.get(frame.__class__, {}).items():
                try:
                    maybe_coro = handler(frame)
                    if maybe_coro is not None:
                        await maybe_coro
                except Exception:
                    logger.exception("Error while running handler %s of frame %s", handler, frame)

    def _start_heartbeat(self) -> None:
        self.start_task("heartbeat_sender", self._heartbeat_sender())
        self._last_heartbeat = time.monotonic()

    async def _heartbeat_sender(self) -> None:
        if self._heartbeat == 0:
            return

        while True:
            await self.send_frame(schema.Heartbeat())
            await asyncio.sleep(self._heartbeat)

            if time.monotonic() - self._last_heartbeat > self._heartbeat * 2:
                logger.warning("Heartbeats from server missing")

    def _on_heartbeat(self, _: schema.Heartbeat) -> None:
        self._last_heartbeat = time.monotonic()

    def _on_close(self, close: schema.Close) -> None:
        exc = exceptions.ServerError(f"Server closed, reason: {close.reason}")
        for waiters in self._waiters.values():
            for fut in waiters:
                fut.set_exception(exc)
        raise exc

    @property
    def is_started(self) -> bool:
        return "heartbeat_sender" in self._tasks

    async def close(self) -> None:
        logger.info("Stopping client %s:%s", self.host, self.port)
        if self._conn is None:
            return

        if self.is_started:
            await self.sync_request(
                schema.Close(
                    self._corr_id_seq.next(),
                    code=1,
                    reason="OK",
                ),
                resp_schema=schema.CloseResponse,
            )

        await self.stop_task("listener")

        await self._conn.close()
        self._conn = None

        self.server_properties = None
        self._tasks.clear()
        self._handlers.clear()


class Client(BaseClient):
    async def close(self) -> None:
        await self.stop_task("heartbeat_sender")
        await super().close()

    async def peer_properties(self) -> dict[str, str]:
        resp = await self.sync_request(
            schema.PeerProperties(
                correlation_id=self._corr_id_seq.next(),
                properties=[schema.Property(key, value) for key, value in self._client_properties.items()],
            ),
            resp_schema=schema.PeerPropertiesResponse,
        )
        return {prop.key: prop.value for prop in resp.properties}

    async def open(self, vhost: str) -> dict[str, str]:
        resp = await self.sync_request(
            schema.Open(
                self._corr_id_seq.next(),
                virtual_host=vhost,
            ),
            resp_schema=schema.OpenResponse,
        )
        return {prop.key: prop.value for prop in resp.properties}

    async def authenticate(self, vhost: str, username: str, password: str) -> dict[str, str]:
        self.server_properties = await self.peer_properties()

        handshake_resp = await self.sync_request(
            schema.SaslHandshake(
                correlation_id=self._corr_id_seq.next(),
            ),
            resp_schema=schema.SaslHandshakeResponse,
        )
        assert "PLAIN" in handshake_resp.mechanisms

        auth_data = b"".join(
            (
                b"\0",
                username.encode("utf-8"),
                b"\0",
                password.encode("utf-8"),
            )
        )
        waiter = self.wait_frame(schema.Tune)
        await self.sync_request(
            schema.SaslAuthenticate(
                self._corr_id_seq.next(),
                mechanism="PLAIN",
                data=auth_data,
            ),
            resp_schema=schema.SaslAuthenticateResponse,
        )
        tune_req = await waiter

        self._frame_max = min(self._frame_max, tune_req.frame_max)
        self._heartbeat = min(self._heartbeat, tune_req.heartbeat)

        await self.send_frame(
            schema.Tune(
                frame_max=self._frame_max,
                heartbeat=self._heartbeat,
            ),
        )
        self._start_heartbeat()

        open_response = await self.open(vhost)
        self.server_properties.update(open_response)
        return open_response

    async def create_stream(self, stream: str, arguments: Optional[dict[str, Any]] = None) -> None:
        if arguments is None:
            arguments = {}

        await self.sync_request(
            schema.Create(
                self._corr_id_seq.next(),
                stream=stream,
                arguments=[schema.Property(key, str(val)) for key, val in arguments.items()],
            ),
            resp_schema=schema.CreateResponse,
        )

    async def delete_stream(self, stream: str) -> None:
        await self.sync_request(
            schema.Delete(
                self._corr_id_seq.next(),
                stream=stream,
            ),
            resp_schema=schema.DeleteResponse,
        )

    async def stream_exists(self, stream: str) -> bool:
        try:
            await self.sync_request(
                schema.Metadata(
                    self._corr_id_seq.next(),
                    streams=[stream],
                ),
                resp_schema=schema.MetadataResponse,
            )
        except exceptions.StreamDoesNotExist:
            return False
        return True

    async def query_leader_and_replicas(
        self,
        stream: str,
    ) -> tuple[schema.Broker, list[schema.Broker]]:
        metadata_resp = await self.sync_request(
            schema.Metadata(
                self._corr_id_seq.next(),
                streams=[stream],
            ),
            resp_schema=schema.MetadataResponse,
        )
        assert len(metadata_resp.metadata) == 1
        metadata = metadata_resp.metadata[0]
        assert metadata.name == stream

        brokers = {broker.reference: broker for broker in metadata_resp.brokers}
        leader = brokers[metadata.leader_ref]
        replicas = [brokers[replica_ref] for replica_ref in metadata.replicas_refs]
        return leader, replicas

    async def subscribe(
        self,
        stream: str,
        subscription_id: int,
        offset_spec: schema.OffsetSpec = schema.OffsetSpec(constants.OffsetType.FIRST),
        initial_credit: int = 10,
        properties: Optional[dict[str, Any]] = None,
    ) -> None:
        await self.sync_request(
            schema.Subscribe(
                self._corr_id_seq.next(),
                subscription_id=subscription_id,
                stream=stream,
                offset_spec=offset_spec,
                credit=initial_credit,
                properties=[schema.Property(key, str(value)) for key, value in (properties or {}).items()],
            ),
            resp_schema=schema.SubscribeResponse,
        )

    async def unsubscribe(self, subscription_id: int) -> None:
        await self.sync_request(
            schema.Unsubscribe(
                self._corr_id_seq.next(),
                subscription_id=subscription_id,
            ),
            resp_schema=schema.UnsubscribeResponse,
        )

    async def credit(self, subscription_id: int, credit: int) -> None:
        await self.send_frame(
            schema.Credit(
                subscription_id=subscription_id,
                credit=credit,
            )
        )

    async def query_offset(self, stream: str, reference: str) -> int:
        resp = await self.sync_request(
            schema.QueryOffset(
                self._corr_id_seq.next(),
                reference=reference,
                stream=stream,
            ),
            resp_schema=schema.QueryOffsetResponse,
        )
        resp.check_response_code(True)
        return resp.offset

    async def store_offset(self, stream: str, reference: str, offset: int) -> None:
        await self.send_frame(
            schema.StoreOffset(
                reference=reference,
                stream=stream,
                offset=offset,
            )
        )

    async def declare_publisher(self, stream: str, reference: str, publisher_id: int) -> None:
        await self.sync_request(
            schema.DeclarePublisher(
                self._corr_id_seq.next(),
                publisher_id=publisher_id,
                stream=stream,
                reference=reference,
            ),
            resp_schema=schema.DeclarePublisherResponse,
            raise_exception=False,
        )

    async def delete_publisher(self, publisher_id: int) -> None:
        await self.sync_request(
            schema.DeletePublisher(
                self._corr_id_seq.next(),
                publisher_id=publisher_id,
            ),
            resp_schema=schema.DeletePublisherResponse,
        )

    async def query_publisher_sequence(self, stream: str, reference: str) -> int:
        resp = await self.sync_request(
            schema.QueryPublisherSequence(
                self._corr_id_seq.next(),
                publisher_ref=reference,
                stream=stream,
            ),
            resp_schema=schema.QueryPublisherSequenceResponse,
            raise_exception=False,
        )
        return resp.sequence

    async def publish(self, messages: list[schema.Message], publisher_id: int) -> None:
        await self.send_frame(
            schema.Publish(
                publisher_id=publisher_id,
                messages=messages,
            ),
        )

    async def partitions(self, super_stream: str) -> list[str]:
        resp = await self.sync_request(
            schema.SuperStreamPartitions(
                self._corr_id_seq.next(),
                super_stream=super_stream,
            ),
            resp_schema=schema.SuperStreamPartitionsResponse,
            raise_exception=False,
        )
        return resp.streams

    async def consumer_update(self, correlation_id: int, offset_specification: OffsetSpecification) -> None:

        await self.send_frame(
            schema.ConsumerUpdateServerResponse(
                correlation_id=correlation_id,
                response_code=1,
                offset_specification=offset_specification,
            ),
        )

    async def route(self, routing_key: str, super_stream: str) -> list[str]:
        resp = await self.sync_request(
            schema.SuperStreamRoute(
                self._corr_id_seq.next(),
                routing_key=routing_key,
                super_stream=super_stream,
            ),
            resp_schema=schema.SuperStreamRouteResponse,
            raise_exception=False,
        )
        return resp.streams


class ClientPool:
    def __init__(
        self,
        host: str,
        port: int,
        *,
        ssl_context: Optional[ssl.SSLContext] = None,
        vhost: str,
        username: str,
        password: str,
        frame_max: int,
        heartbeat: int,
        load_balancer_mode: bool,
        max_retries: int,
    ):
        self.addr = Addr(host=host, port=port)
        self.ssl_context = ssl_context
        self.load_balancer_mode = load_balancer_mode
        self.max_retries = max_retries
        self.vhost = vhost
        self.username = username
        self.password = password

        self._frame_max = frame_max
        self._heartbeat = heartbeat
        self._clients: dict[Addr, Client] = {}

    async def get(
        self, addr: Optional[Addr] = None, connection_closed_handler: Optional[CB[Exception]] = None
    ) -> Client:
        """Get a client according to `addr` parameter

        If class param `load_balancer_mode` is True, we create a connection via the LB
        in a loop until the desired node is returned.

        If no `addr` is supplied, then it is assumed the exact node is not important
        and `load_balancer_mode` is ignored.
        """
        desired_addr = addr or self.addr

        if desired_addr not in self._clients:
            if addr and self.load_balancer_mode:
                self._clients[desired_addr] = await self._resolve_broker(
                    desired_addr, connection_closed_handler
                )
            else:
                self._clients[desired_addr] = await self.new(
                    addr=desired_addr, connection_closed_handler=connection_closed_handler
                )

        assert self._clients[desired_addr].is_started
        return self._clients[desired_addr]

    async def _resolve_broker(
        self, addr: Addr, connection_closed_handler: Optional[CB[Exception]] = None
    ) -> Client:
        desired_host, desired_port = addr.host, str(addr.port)

        connection_attempts = 0

        while connection_attempts < self.max_retries:
            client = await self.new(addr=self.addr, connection_closed_handler=connection_closed_handler)

            assert client.server_properties is not None

            advertised_host = client.server_properties["advertised_host"]
            advertised_port = client.server_properties["advertised_port"]

            if advertised_host == desired_host and advertised_port == desired_port:
                return client

            connection_attempts += 1

            await client.close()
            await asyncio.sleep(0.2)

        raise BrokerResolutionMaxRetryError(
            f"Failed to connect to {desired_host}:{desired_port} after {self.max_retries} tries"
        )

    async def new(self, addr: Addr, connection_closed_handler: Optional[CB[Exception]] = None) -> Client:
        host, port = addr
        client = Client(
            host=host,
            port=port,
            ssl_context=self.ssl_context,
            frame_max=self._frame_max,
            heartbeat=self._heartbeat,
            connection_closed_handler=connection_closed_handler,
        )
        await client.start()
        await client.authenticate(
            vhost=self.vhost,
            username=self.username,
            password=self.password,
        )
        return client

    async def close(self) -> None:
        for client in self._clients.values():
            await client.close()

        self._clients.clear()
