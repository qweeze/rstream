# Copyright 2023 VMware, Inc. All Rights Reserved.
# SPDX-License-Identifier: MIT

import asyncio
import logging
from collections import defaultdict
from typing import Any, Awaitable, Callable, Optional

from rstream import (
    AMQPMessage,
    ConfirmationStatus,
    Consumer,
    EventContext,
    MessageContext,
    OffsetSpecification,
    OffsetType,
    Producer,
    SuperStreamConsumer,
    amqp_decoder,
)

from .http_requests import (
    delete_connection,
    get_connection,
    get_connection_present,
    get_connections,
)

captured: list[bytes] = []
logger = logging.getLogger(__name__)


async def wait_for(condition, timeout=1):
    async def _wait():
        while not condition():
            await asyncio.sleep(0.01)

    await asyncio.wait_for(_wait(), timeout)


async def consumer_update_handler_next(is_active: bool, event_context: EventContext) -> OffsetSpecification:
    return OffsetSpecification(OffsetType.NEXT, 0)


async def consumer_update_handler_first(is_active: bool, event_context: EventContext) -> OffsetSpecification:
    return OffsetSpecification(OffsetType.FIRST, 0)


async def consumer_update_handler_offset(is_active: bool, event_context: EventContext) -> OffsetSpecification:
    return OffsetSpecification(OffsetType.OFFSET, 10)


async def on_publish_confirm_client_callback(
    confirmation: ConfirmationStatus, confirmed_messages: list[int], errored_messages: list[int]
) -> None:
    if confirmation.is_confirmed is True:
        confirmed_messages.append(confirmation.message_id)
    else:
        errored_messages.append(confirmation.message_id)


async def on_publish_confirm_client_callback2(
    confirmation: ConfirmationStatus, confirmed_messages: list[int], errored_messages: list[int]
) -> None:
    if confirmation.is_confirmed is True:
        confirmed_messages.append(confirmation.message_id)
    else:
        errored_messages.append(confirmation.message_id)


async def routing_extractor(message: AMQPMessage) -> str:
    return "0"


async def routing_extractor_generic(message: AMQPMessage) -> str:
    return message.application_properties["id"]


async def routing_extractor_for_sac(message: AMQPMessage) -> str:
    return str(message.properties.message_id)


async def routing_extractor_key(message: AMQPMessage) -> str:
    return "key1"


async def on_message(
    msg: AMQPMessage, message_context: MessageContext, streams: list[str], offsets: list[int]
):
    stream = message_context.consumer.get_stream(message_context.subscriber_name)
    streams.append(stream)
    offset = message_context.offset
    offsets.append(offset)


async def on_message_sac(msg: AMQPMessage, message_context: MessageContext, streams: list[str]):
    stream = message_context.consumer.get_stream(message_context.subscriber_name)
    streams.append(stream)


async def run_consumer(
    super_stream_consumer: SuperStreamConsumer,
    streams: list[str],
    consumer_update_listener: Optional[Callable[[bool, EventContext], Awaitable[Any]]] = None,
):
    properties: dict[str, str] = defaultdict(str)
    properties["single-active-consumer"] = "true"
    properties["name"] = "consumer-group-1"
    properties["super-stream"] = "test-super-stream"

    await super_stream_consumer.subscribe(
        callback=lambda message, message_context: streams.append(
            message_context.consumer.get_stream(message_context.subscriber_name)
        ),
        decoder=amqp_decoder,
        properties=properties,
        consumer_update_listener=consumer_update_listener,
    )


async def task_to_delete_connection(connection_name: str) -> None:
    # delay a few seconds before deleting the connection
    await asyncio.sleep(5)

    connections = get_connections()

    await wait_for(lambda: get_connection_present(connection_name, connections) is True)

    for connection in connections:
        if connection["client_properties"]["connection_name"] == connection_name:
            delete_connection(connection["name"])
            await wait_for(lambda: get_connection(connection["name"]) is False, 2)


async def task_to_delete_stream_producer(producer: Producer, stream: str) -> None:
    # delay a few seconds before deleting the connection
    await asyncio.sleep(4)

    await producer.delete_stream(stream)


async def task_to_delete_stream_consumer(consumer: Consumer, stream: str) -> None:
    # delay a few seconds before deleting the connection
    await asyncio.sleep(4)

    await consumer.delete_stream(stream)


async def filter_value_extractor(message: AMQPMessage) -> str:
    return message.application_properties["id"]
