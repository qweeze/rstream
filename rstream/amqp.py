from __future__ import annotations

from typing import Any, Optional, Protocol, cast

import uamqp


class _MessageProtocol(Protocol):
    publishing_id: Optional[int] = None

    def __bytes__(self) -> bytes:
        ...


class AMQPMessage(uamqp.Message, _MessageProtocol):
    def __init__(self, *args: Any, publishing_id: Optional[int] = None, **kwargs: Any):
        self.publishing_id = publishing_id
        super().__init__(*args, **kwargs)

    def __bytes__(self) -> bytes:
        return cast(bytes, self.encode_message())


def amqp_decoder(data: bytes) -> AMQPMessage:
    message = AMQPMessage.decode_from_bytes(data)
    return message
