# Copyright 2023 VMware, Inc. All Rights Reserved.
# SPDX-License-Identifier: MIT

import zlib
from dataclasses import dataclass, field
from typing import ClassVar, Optional, Type, cast

from .constants import Key, OffsetType, T
from .exceptions import ServerError
from .compression import CompressionType, CompressionHelper, ICompressionCodec

registry: dict[tuple[bool, Key], Type["Frame"]] = {}


@dataclass
class Struct:
    ...


@dataclass
class Frame(Struct):
    key: ClassVar[Key] = NotImplemented
    version: ClassVar[int] = 1

    @property
    def corr_id(self) -> Optional[int]:
        correlation_id = getattr(self, "correlation_id", None)
        if correlation_id is not None:
            return cast(int, correlation_id)
        return None

    def __init_subclass__(cls, is_response: bool = False) -> None:
        assert cls.key is not NotImplemented
        registry[(is_response, cls.key)] = cls

    def check_response_code(self, raise_exception: bool = True) -> None:
        code: int = getattr(self, "response_code", 0)
        if code > 1 and raise_exception is True:
            raise ServerError.from_code(code)


@dataclass
class Property(Struct):
    key: str = field(metadata={"type": T.string})
    value: str = field(metadata={"type": T.string})


@dataclass
class PeerProperties(Frame):
    key = Key.PeerProperties
    correlation_id: int = field(metadata={"type": T.uint32})
    properties: list[Property]


@dataclass
class PeerPropertiesResponse(Frame, is_response=True):
    key = Key.PeerProperties
    correlation_id: int = field(metadata={"type": T.uint32})
    response_code: int = field(metadata={"type": T.uint16})
    properties: list[Property]


@dataclass
class SaslHandshake(Frame):
    key = Key.SaslHandshake
    correlation_id: int = field(metadata={"type": T.uint32})


@dataclass
class SaslHandshakeResponse(Frame, is_response=True):
    key = Key.SaslHandshake
    correlation_id: int = field(metadata={"type": T.uint32})
    response_code: int = field(metadata={"type": T.uint16})
    mechanisms: list[str] = field(metadata={"type": [T.string]})


@dataclass
class SaslAuthenticate(Frame):
    key = Key.SaslAuthenticate
    correlation_id: int = field(metadata={"type": T.uint32})
    mechanism: str = field(metadata={"type": T.string})
    data: bytes = field(metadata={"type": T.bytes})


@dataclass
class SaslAuthenticateResponse(Frame, is_response=True):
    key = Key.SaslAuthenticate
    correlation_id: int = field(metadata={"type": T.uint32})
    response_code: int = field(metadata={"type": T.uint16})


@dataclass
class Tune(Frame):
    key = Key.Tune
    frame_max: int = field(metadata={"type": T.int32})
    heartbeat: int = field(metadata={"type": T.int32})


@dataclass
class Open(Frame):
    key = Key.Open
    correlation_id: int = field(metadata={"type": T.uint32})
    virtual_host: str = field(metadata={"type": T.string})


@dataclass
class OpenResponse(Frame, is_response=True):
    key = Key.Open
    correlation_id: int = field(metadata={"type": T.uint32})
    response_code: int = field(metadata={"type": T.uint16})
    properties: list[Property]


@dataclass
class Heartbeat(Frame):
    key = Key.Heartbeat


@dataclass
class Create(Frame):
    key = Key.Create
    correlation_id: int = field(metadata={"type": T.uint32})
    stream: str = field(metadata={"type": T.string})
    arguments: list[Property]


@dataclass
class CreateResponse(Frame, is_response=True):
    key = Key.Create
    correlation_id: int = field(metadata={"type": T.uint32})
    response_code: int = field(metadata={"type": T.uint16})


@dataclass
class Delete(Frame):
    key = Key.Delete
    correlation_id: int = field(metadata={"type": T.uint32})
    stream: str = field(metadata={"type": T.string})


@dataclass
class DeleteResponse(Frame, is_response=True):
    key = Key.Delete
    correlation_id: int = field(metadata={"type": T.uint32})
    response_code: int = field(metadata={"type": T.uint16})


@dataclass
class DeclarePublisher(Frame):
    key = Key.DeclarePublisher
    correlation_id: int = field(metadata={"type": T.uint32})
    publisher_id: int = field(metadata={"type": T.uint8})
    reference: str = field(metadata={"type": T.string})
    stream: str = field(metadata={"type": T.string})


@dataclass
class DeclarePublisherResponse(Frame, is_response=True):
    key = Key.DeclarePublisher
    correlation_id: int = field(metadata={"type": T.uint32})
    response_code: int = field(metadata={"type": T.uint16})


@dataclass
class QueryPublisherSequence(Frame):
    key = Key.QueryPublisherSequence
    correlation_id: int = field(metadata={"type": T.uint32})
    publisher_ref: str = field(metadata={"type": T.string})
    stream: str = field(metadata={"type": T.string})


@dataclass
class QueryPublisherSequenceResponse(Frame, is_response=True):
    key = Key.QueryPublisherSequence
    correlation_id: int = field(metadata={"type": T.uint32})
    response_code: int = field(metadata={"type": T.uint16})
    sequence: int = field(metadata={"type": T.uint64})


@dataclass
class Message(Struct):
    publishing_id: int = field(metadata={"type": T.uint64})
    data: bytes = field(metadata={"type": T.bytes})

@dataclass
class Publish(Frame):
    key = Key.Publish
    publisher_id: int = field(metadata={"type": T.uint8})
    messages: list[Message]


@dataclass
class PublishSubBatching(Frame):
    key = Key.Publish
    publisher_id: int = field(metadata={"type": T.uint8})
    number_of_root_messages: int = field(metadata={"type": T.int32})
    publishing_id: int = field(metadata={"type": T.uint64})
    compress_type: int = field(metadata={"type": T.uint8})
    subbatching_message_count: int = field(metadata={"type": T.uint16})
    uncompressed_data_size: int = field(metadata={"type": T.int32})
    compressed_data_size: int = field(metadata={"type": T.int32})
    messages: bytes = field(metadata={"type": T.raw})


@dataclass
class PublishConfirm(Frame):
    key = Key.PublishConfirm
    publisher_id: int = field(metadata={"type": T.uint8})
    publishing_ids: list[int] = field(metadata={"type": [T.uint64]})


@dataclass
class PublishingError(Struct):
    publishing_id: int = field(metadata={"type": T.uint64})
    response_code: int = field(metadata={"type": T.uint16})


@dataclass
class PublishError(Frame):
    key = Key.PublishError
    publisher_id: int = field(metadata={"type": T.uint8})
    errors: list[PublishingError]


@dataclass
class Metadata(Frame):
    key = Key.Metadata
    correlation_id: int = field(metadata={"type": T.uint32})
    streams: list[str] = field(metadata={"type": [T.string]})


@dataclass
class Broker(Struct):
    reference: int = field(metadata={"type": T.uint16})
    host: str = field(metadata={"type": T.string})
    port: int = field(metadata={"type": T.uint32})


@dataclass
class StreamMetadata(Struct):
    name: str = field(metadata={"type": T.string})
    response_code: int = field(metadata={"type": T.uint16})
    leader_ref: int = field(metadata={"type": T.uint16})
    replicas_refs: list[int] = field(metadata={"type": [T.uint16]})


@dataclass
class MetadataResponse(Frame, is_response=True):
    key = Key.Metadata
    correlation_id: int = field(metadata={"type": T.uint32})
    brokers: list[Broker]
    metadata: list[StreamMetadata]

    def check_response_code(self, raise_exception: bool = True) -> None:
        for item in self.metadata:
            code = item.response_code
            if code > 1 and raise_exception is True:
                raise ServerError.from_code(code)


@dataclass
class MetadataInfo(Struct):
    code: int = field(metadata={"type": T.uint16})
    stream: str = field(metadata={"type": T.string})


@dataclass
class MetadataUpdate(Frame):
    key = Key.MetadataUpdate
    metadata_info: MetadataInfo


@dataclass
class DeletePublisher(Frame):
    key = Key.DeletePublisher
    correlation_id: int = field(metadata={"type": T.uint32})
    publisher_id: int = field(metadata={"type": T.uint8})


@dataclass
class DeletePublisherResponse(Frame, is_response=True):
    key = Key.DeletePublisher
    correlation_id: int = field(metadata={"type": T.uint32})
    response_code: int = field(metadata={"type": T.uint16})


@dataclass
class Close(Frame):
    key = Key.Close
    correlation_id: int = field(metadata={"type": T.uint32})
    code: int = field(metadata={"type": T.uint16})
    reason: str = field(metadata={"type": T.string})


@dataclass
class CloseResponse(Frame, is_response=True):
    key = Key.Close
    correlation_id: int = field(metadata={"type": T.uint32})
    response_code: int = field(metadata={"type": T.uint16})


@dataclass
class OffsetSpec(Struct):
    offset_type: OffsetType = field(metadata={"type": T.uint16})

    @classmethod
    def from_params(cls, offset_type: OffsetType, offset: Optional[int]) -> "OffsetSpec":
        if offset_type in (OffsetType.OFFSET, OffsetType.TIMESTAMP) and offset is None:
            raise ValueError(f"Offset parameter is required for {offset_type} offset type")
        elif offset_type in (OffsetType.NEXT, OffsetType.FIRST, OffsetType.LAST) and offset is not None:
            raise ValueError(f"Offset parameter must be None for {offset_type} offset type")

        if offset_type is OffsetType.OFFSET:
            assert offset is not None
            return OffsetSpecOffset(offset_type, offset)

        elif offset_type is OffsetType.TIMESTAMP:
            assert offset is not None
            return OffsetSpecTimestamp(offset_type, offset)
        else:
            assert offset is None
            return OffsetSpec(offset_type)


@dataclass
class OffsetSpecOffset(OffsetSpec):
    value: int = field(metadata={"type": T.uint64})


@dataclass
class OffsetSpecTimestamp(OffsetSpec):
    value: int = field(metadata={"type": T.int64})


@dataclass
class Subscribe(Frame):
    key = Key.Subscribe
    correlation_id: int = field(metadata={"type": T.uint32})
    subscription_id: int = field(metadata={"type": T.uint8})
    stream: str = field(metadata={"type": T.string})
    offset_spec: OffsetSpec
    credit: int = field(metadata={"type": T.uint16})
    properties: list[Property]


@dataclass
class SubscribeResponse(Frame, is_response=True):
    key = Key.Subscribe
    correlation_id: int = field(metadata={"type": T.uint32})
    response_code: int = field(metadata={"type": T.uint16})


@dataclass
class Unsubscribe(Frame):
    key = Key.Unsubscribe
    correlation_id: int = field(metadata={"type": T.uint32})
    subscription_id: int = field(metadata={"type": T.uint8})


@dataclass
class UnsubscribeResponse(Frame, is_response=True):
    key = Key.Unsubscribe
    correlation_id: int = field(metadata={"type": T.uint32})
    response_code: int = field(metadata={"type": T.uint16})


@dataclass
class StoreOffset(Frame):
    key = Key.StoreOffset
    reference: str = field(metadata={"type": T.string})
    stream: str = field(metadata={"type": T.string})
    offset: int = field(metadata={"type": T.uint64})


@dataclass
class QueryOffset(Frame):
    key = Key.QueryOffset
    correlation_id: int = field(metadata={"type": T.uint32})
    reference: str = field(metadata={"type": T.string})
    stream: str = field(metadata={"type": T.string})


@dataclass
class QueryOffsetResponse(Frame, is_response=True):
    key = Key.QueryOffset
    correlation_id: int = field(metadata={"type": T.uint32})
    response_code: int = field(metadata={"type": T.uint16})
    offset: int = field(metadata={"type": T.uint64})

@dataclass
class SubEntryChunk:
    compress_value: bytes
    num_records_in_batch: int
    uncompressed_data_size: int
    message_count: int        
    data_len: int
    
    @classmethod
    def read(self, data:bytes, entry_type:bytes, pos:int) -> int:

        self.num_records_in_batch=int.from_bytes(data[pos:2])
        pos+=2
        self.uncompressed_data_size=int.from_bytes(data[pos:pos+4])
        pos+=4
        self.data_len=int.from_bytes(data[pos:pos+4])
        pos+=4
        
        print("entry_type: " + str(entry_type))
        print("num record in batch" + str(self.num_records_in_batch))
        print("uncompressed_data_size" + str(self.uncompressed_data_size))
        print("data_len" + str(self.data_len))
        
        compression_type = CompressionType((entry_type & 0x70) >> 4)
        print("compression: " + str(compression_type))
        
        data = data[pos:]
        uncompressed_data=CompressionHelper.uncompress(data, compression_type=compression_type, uncompressed_data_size=self.uncompressed_data_size)
        
        messages = []
        
        for i in range(self.num_records_in_batch):
            size = int.from_bytes(uncompressed_data[pos:pos + 4], "big")
            pos += 4
            messages.append(uncompressed_data[pos:pos + size])
            pos += size
            
        return messages
        

@dataclass
class Deliver(Frame):
    key = Key.Deliver
    subscription_id: int = field(metadata={"type": T.uint8})
    magic_version: int = field(metadata={"type": T.int8})
    chunk_type: int = field(metadata={"type": T.int8})
    num_entries: int = field(metadata={"type": T.uint16})
    num_records: int = field(metadata={"type": T.uint32})
    timestamp: int = field(metadata={"type": T.int64})
    epoch: int = field(metadata={"type": T.uint64})
    chunk_first_offset: int = field(metadata={"type": T.uint64})
    chunk_crc: int = field(metadata={"type": T.uint32})
    data_length: int = field(metadata={"type": T.uint32})
    trailer_length: int = field(metadata={"type": T.uint32})
    _reserved: int = field(metadata={"type": T.uint32})
    data: bytes = field(metadata={"type": T.raw})

    def __post_init__(self) -> None:
        if self.data_length != len(self.data):
            raise ValueError("Invalid frame")

        if self.chunk_type != 0:
            raise ValueError("Unknown chunk type: %s", self.chunk_type)

        if zlib.crc32(self.data) != self.chunk_crc:
            raise ValueError("Invalid checksum")

    def get_messages(self) -> list[bytes]:
        messages = []
        pos = 0
       
        for _ in range(self.num_entries):
            entry_type = self.data[pos] & 0x80
            if (entry_type == 0):
                size = int.from_bytes(self.data[pos:pos + 4], "big")
                pos += 4
                messages.append(self.data[pos:pos + size])
                pos += size
            else:
                #raise NotImplementedError
                size = int.from_bytes(self.data[pos:pos + 4], "big")
                pos += 4
                messages_subbatch = SubEntryChunk.read(self.data, entry_type, pos)
                messages.append(messages_subbatch)
                
        return messages


@dataclass
class Credit(Frame):
    key = Key.Credit
    subscription_id: int = field(metadata={"type": T.uint8})
    credit: int = field(metadata={"type": T.uint16})


@dataclass
class CreditResponse(Frame, is_response=True):
    key = Key.Credit
    response_code: int = field(metadata={"type": T.uint16})
    subscription_id: int = field(metadata={"type": T.uint8})
    

