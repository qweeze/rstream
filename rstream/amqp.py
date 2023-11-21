from __future__ import annotations

from typing import Any, Optional, Protocol, cast

#import uamqp

class Message:

    def __init__(self, body: bytearray):
        self._body: bytearray = body

    def encode_message(self) -> bytes:
        message_bytes = bytearray(4)
        message_bytes[0] = 0x00
        message_bytes[1] = 0x53
        message_bytes[2] = 0x75
        message_bytes[3] = 0xb0
        length = len(self._body)
        message_len = length.to_bytes(4, "big", signed=False)
        message_body = self._body

        return b"".join(
            (
                message_bytes,
                message_len,
                message_body,
            )
        )


    @staticmethod
    def decode_from_bytes(data: bytes) -> Message:
        return Message(body=bytearray(data))


class _MessageProtocol(Protocol):
    publishing_id: Optional[int] = None

    def __bytes__(self) -> bytes:
        ...


class AMQPMessage(Message, _MessageProtocol):
    def __init__(self, *args: Any, publishing_id: Optional[int] = None, **kwargs: Any):
        self.publishing_id = publishing_id
        super().__init__(*args, **kwargs)

    def __bytes__(self) -> bytes:
        return cast(bytes, self.encode_message())


def amqp_decoder(data: bytes) -> AMQPMessage:
    message = AMQPMessage.decode_from_bytes(data=data)
    return message






