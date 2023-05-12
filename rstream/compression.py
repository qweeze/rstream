# Copyright 2023 VMware, Inc. All Rights Reserved.
# SPDX-License-Identifier: MIT

from enum import Enum
import abc
from .amqp import _MessageProtocol
from collections import defaultdict
import gzip
from dataclasses import dataclass
from . import utils
from .utils import RawMessage

from typing import (
    TypeVar,
    Optional,
)

MessageT = TypeVar("MessageT", _MessageProtocol, bytes)


class CompressionType(Enum):
    No = 0
    Gzip = 1
    Snappy = 2
    Lz4 = 3
    Zstd = 4


class ICompressionCodec(abc.ABC):
    @abc.abstractmethod
    def compress(messages: list[MessageT]):
        pass

    @abc.abstractmethod
    def compressed_size() -> int:
        pass

    @abc.abstractmethod
    def uncompressed_size() -> int:
        pass

    @abc.abstractmethod
    def messages_count() -> int:
        pass

    @abc.abstractmethod
    def compression_type() -> int:
        pass


class NoneCompressionCodec(ICompressionCodec):

    def __init__(self):
        self.uncompressed_data_size = 0
        self.compressed_data_size = 0
        self.message_count = 0
        self.buffer = bytes()

    def compress(self, messages: list[MessageT]):
        for item in messages:
            msg = RawMessage(item) if isinstance(item, bytes) else item
            tmp = bytes(msg)
            self.buffer += len(tmp).to_bytes(4, "big")
            self.buffer += tmp

        self.message_count = len(messages)
        self.uncompressed_data_size = len(self.buffer)
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


class GzipCompressionCodec(ICompressionCodec):

    def __init__(self):
        self.uncompressed_data_size = 0
        self.compressed_data_size = 0
        self.message_count = 0
        self.buffer = bytes()

    def compress(self, messages: list[MessageT]):

        for item in messages:
            msg = RawMessage(item) if isinstance(item, bytes) else item
            tmp = bytes(msg)
            self.buffer += len(tmp).to_bytes(4, "big")
            self.buffer += tmp

        self.message_count = len(messages)
        self.uncompressed_data_size = len(self.buffer)
        compressed_buffer = gzip.compress(self.buffer)
        self.compressed_data_size = len(compressed_buffer)
        self.buffer = compressed_buffer

    def uncompress(self, compressed_data: bytes, uncompressed_data_size: int) -> bytes:

        uncompressed_data = gzip.decompress(compressed_data)

        if len(uncompressed_data) != uncompressed_data_size:
            print("uncompressed len is different")

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
        return StreamCompressionCodecs.available_compress_codecs[compression_type]


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
