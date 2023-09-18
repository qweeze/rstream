# Copyright 2023 VMware, Inc. All Rights Reserved.
# SPDX-License-Identifier: MIT

import asyncio
from collections import defaultdict
from typing import Any, Awaitable, Callable, Optional

from rstream import (
    AMQPMessage,
    ConfirmationStatus,
    EventContext,
    MessageContext,
    OffsetSpecification,
    OffsetType,
    SuperStreamConsumer,
    amqp_decoder,
)

captured: list[bytes] = []


async def consumer_update_handler_next(is_active: bool, event_context: EventContext) -> OffsetSpecification:

    return OffsetSpecification(OffsetType.NEXT, 0)


async def consumer_update_handler_first(is_active: bool, event_context: EventContext) -> OffsetSpecification:

    return OffsetSpecification(OffsetType.FIRST, 0)


async def consumer_update_handler_offset(is_active: bool, event_context: EventContext) -> OffsetSpecification:

    return OffsetSpecification(OffsetType.OFFSET, 10)


async def wait_for(condition, timeout=1):
    async def _wait():
        while not condition():
            await asyncio.sleep(0.01)

    await asyncio.wait_for(_wait(), timeout)


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
