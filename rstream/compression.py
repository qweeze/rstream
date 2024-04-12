# Copyright 2023 VMware, Inc. All Rights Reserved.
# SPDX-License-Identifier: MIT

import abc
import copy
import gzip
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
from typing import TypeVar

from .amqp import _MessageProtocol
from .utils import RawMessage

MessageT = TypeVar("MessageT", _MessageProtocol, bytes)


class CompressionType(Enum):
    No = 0
    Gzip = 1
    Snappy = 2
    Lz4 = 3
    Zstd = 4


class ICompressionCodec(abc.ABC):
    @abc.abstractmethod
    def compress(self, messages: list[MessageT]):
        pass

    @abc.abstractmethod
    def uncompress(self, compressed_data: bytes, uncompressed_data_size: int) -> bytes:
        pass

    @abc.abstractmethod
    def compressed_size(self) -> int:
        pass

    @abc.abstractmethod
    def uncompressed_size(self) -> int:
        pass

    @abc.abstractmethod
    def messages_count(self) -> int:
        pass

    @abc.abstractmethod
    def compression_type(self) -> int:
        pass

    @abc.abstractmethod
    def data(self) -> bytes:
        pass


@dataclass
class NoneCompressionCodec(ICompressionCodec):
    uncompressed_data_size: int = 0
    compressed_data_size: int = 0
    message_count: int = 0
    buffer: bytes = bytes()

    def compress(self, messages: list[MessageT]):
        uncompressed_data = bytes()
        for item in messages:
            msg = RawMessage(item) if isinstance(item, bytes) else item
            msg_buffer = bytes(msg)
            uncompressed_data += len(msg_buffer).to_bytes(4, "big")
            uncompressed_data += msg_buffer

        self.message_count = len(messages)
        self.uncompressed_data_size = len(uncompressed_data)
        self.buffer = uncompressed_data
        self.compressed_data_size = self.uncompressed_data_size

    def uncompress(self, compressed_data: bytes, uncompressed_data_size: int) -> bytes:
        return compressed_data

    def compressed_size(self) -> int:
        return self.compressed_data_size

    def uncompressed_size(self) -> int:
        return self.uncompressed_data_size

    def messages_count(self) -> int:
        return self.message_count

    def data(self) -> bytes:
        return self.buffer

    def compression_type(self) -> int:
        return 0


@dataclass
class GzipCompressionCodec(ICompressionCodec):
    uncompressed_data_size: int = 0
    compressed_data_size: int = 0
    message_count: int = 0
    buffer: bytes = bytes()

    def compress(self, messages: list[MessageT]):
        uncompressed_data = bytes()
        for item in messages:
            msg = RawMessage(item) if isinstance(item, bytes) else item
            msg_buffer = bytes(msg)
            uncompressed_data += len(msg_buffer).to_bytes(4, "big")
            uncompressed_data += msg_buffer

        self.message_count = len(messages)
        self.uncompressed_data_size = len(uncompressed_data)
        self.buffer = gzip.compress(uncompressed_data)
        self.compressed_data_size = len(self.buffer)

    def uncompress(self, compressed_data: bytes, uncompressed_data_size: int) -> bytes:
        uncompressed_data = gzip.decompress(compressed_data)

        if len(uncompressed_data) != uncompressed_data_size:
            raise ValueError("Uncompressed len error")

        return uncompressed_data

    def compressed_size(self) -> int:
        return self.compressed_data_size

    def uncompressed_size(self) -> int:
        return self.uncompressed_data_size

    def messages_count(self) -> int:
        return self.message_count

    def data(self) -> bytes:
        return self.buffer

    def compression_type(self) -> int:
        return 1


class StreamCompressionCodecs:
    available_compress_codecs: dict[CompressionType, ICompressionCodec] = defaultdict(ICompressionCodec)
    available_compress_codecs[CompressionType.No] = NoneCompressionCodec()
    available_compress_codecs[CompressionType.Gzip] = GzipCompressionCodec()

    @staticmethod
    def register_codec(compression_type: CompressionType, codec: ICompressionCodec):
        StreamCompressionCodecs.available_compress_codecs[compression_type] = codec

    @staticmethod
    def get_compression_codec(compression_type: CompressionType) -> ICompressionCodec:
        return copy.copy(StreamCompressionCodecs.available_compress_codecs[compression_type])


class CompressionHelper:
    @staticmethod
    def compress(messages: list[MessageT], compression_type: CompressionType) -> ICompressionCodec:
        codec = StreamCompressionCodecs.get_compression_codec(compression_type=compression_type)
        codec.compress(messages=messages)
        return codec

    @staticmethod
    def uncompress(data: bytes, compression_type: CompressionType, uncompressed_data_size: int) -> bytes:
        codec = StreamCompressionCodecs.get_compression_codec(compression_type=compression_type)
        messages = codec.uncompress(compressed_data=data, uncompressed_data_size=uncompressed_data_size)
        return messages
