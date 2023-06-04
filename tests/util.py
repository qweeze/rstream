# Copyright 2023 VMware, Inc. All Rights Reserved.
# SPDX-License-Identifier: MIT

import asyncio

from rstream import AMQPMessage, ConfirmationStatus

captured: list[bytes] = []


async def wait_for(condition, timeout=1):
    async def _wait():
        while not condition():
            await asyncio.sleep(0.01)

    await asyncio.wait_for(_wait(), timeout)


def on_publish_confirm_client_callback(
    confirmation: ConfirmationStatus, confirmed_messages: list[int], errored_messages: list[int]
) -> None:

    if confirmation.is_confirmed is True:
        confirmed_messages.append(confirmation.message_id)
    else:
        errored_messages.append(confirmation.message_id)


def on_publish_confirm_client_callback2(
    confirmation: ConfirmationStatus, confirmed_messages: list[int], errored_messages: list[int]
) -> None:

    if confirmation.is_confirmed is True:
        confirmed_messages.append(confirmation.message_id)
    else:
        errored_messages.append(confirmation.message_id)


def routing_extractor(message: AMQPMessage) -> str:
    return "0"
