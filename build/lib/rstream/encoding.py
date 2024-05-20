import io
import logging
import typing
from dataclasses import is_dataclass
from io import BytesIO
from typing import (
    Annotated,
    Any,
    Literal,
    NamedTuple,
    Type,
    Union,
    cast,
)

from .constants import Key, T
from .schema import (
    Frame,
    Publish,
    Struct,
    is_struct,
    registry,
)

__all__ = ["encode_frame", "decode_frame"]
logger = logging.getLogger(__name__)


class IntSpec(NamedTuple):
    length: int
    byteorder: Literal["little", "big"]
    signed: bool


int_specs = {
    T.int8: IntSpec(length=1, byteorder="big", signed=True),
    T.int16: IntSpec(length=2, byteorder="big", signed=True),
    T.int32: IntSpec(length=4, byteorder="big", signed=True),
    T.int64: IntSpec(length=8, byteorder="big", signed=True),
    T.uint8: IntSpec(length=1, byteorder="big", signed=False),
    T.uint16: IntSpec(length=2, byteorder="big", signed=False),
    T.uint32: IntSpec(length=4, byteorder="big", signed=False),
    T.uint64: IntSpec(length=8, byteorder="big", signed=False),
}

_encode_int = {
    T.int8: lambda v: v.to_bytes(length=1, byteorder="big", signed=True),
    T.int16: lambda v: v.to_bytes(length=2, byteorder="big", signed=True),
    T.int32: lambda v: v.to_bytes(length=4, byteorder="big", signed=True),
    T.int64: lambda v: v.to_bytes(length=8, byteorder="big", signed=True),
    T.uint8: lambda v: v.to_bytes(length=1, byteorder="big", signed=False),
    T.uint16: lambda v: v.to_bytes(length=2, byteorder="big", signed=False),
    T.uint32: lambda v: v.to_bytes(length=4, byteorder="big", signed=False),
    T.uint64: lambda v: v.to_bytes(length=8, byteorder="big", signed=False),
}

_VT = Union[int, str, bytes, Struct]
VT = Annotated[Union[_VT, list[_VT]], "Field value"]

_TT = Union[T, Struct, None]
TT = Annotated[Union[_TT, list[_TT]], "Field type metadata"]


def _encode_field(value: VT, tp: TT) -> Union[bytearray, bytes]:
    if isinstance(value, list):
        assert tp is None or isinstance(tp, list)
        buffer = bytearray()
        buffer += len(value).to_bytes(4, "big", signed=False)
        if tp is None:
            for item in value:
                assert isinstance(item, Struct)
                buffer += _encode_struct(item)
        elif len(tp) == 1:
            for item in value:
                buffer += _encode_field(item, tp[0])
        else:
            for item in value:
                assert isinstance(item, list)
                for part, subtype in zip(item, tp):
                    buffer += _encode_field(part, subtype)
        return buffer

    elif isinstance(tp, T) and tp in int_specs:
        assert isinstance(value, int)
        return _encode_int[tp](value)

    elif tp is T.string:
        assert isinstance(value, str)
        buffer = bytearray()
        buffer += len(value).to_bytes(2, "big", signed=False)
        buffer += value.encode("utf-8")
        return buffer

    elif tp is T.bytes:
        assert isinstance(value, bytes)
        buffer = bytearray()
        buffer += len(value).to_bytes(4, "big", signed=False)
        buffer += value
        return buffer

    elif tp is T.raw:
        assert isinstance(value, bytes)
        return value

    elif is_dataclass(value):
        assert isinstance(value, Struct)
        return _encode_struct(value)

    else:
        raise NotImplementedError(f"Unexpected type {tp}, value: {value!r}")


def _encode_struct(struct: Struct) -> bytearray:
    buffer = bytearray()
    for value, tp in struct.iter_typed_values():
        if value is not None:
            buffer += _encode_field(value, tp)
    return buffer


def encode_frame(frame: Frame, version_to_encode: int = 1) -> bytes:
    try:
        payload = _encode_struct(frame)
    except Exception as e:
        raise ValueError(f"Could not encode frame {frame!r}") from e

    length = len(payload) + 2 + 2
    if version_to_encode > 1:
        version = version_to_encode
    else:
        version = frame.version
    return b"".join(
        (
            length.to_bytes(4, "big", signed=False),
            frame.key.value.to_bytes(2, "big", signed=False),
            version.to_bytes(2, "big", signed=False),
            payload,
        )
    )


def encode_publish(frame: Publish, version_to_encode: int = 1) -> bytes:
    with BytesIO() as fp:
        fp_write = fp.write
        fp.seek(8)
        fp_write(frame.publisher_id.to_bytes(length=1, byteorder="big", signed=False))

        messages = frame.messages
        fp_write(len(messages).to_bytes(4, "big", signed=False))
        for msg in messages:
            fp_write(msg.publishing_id.to_bytes(length=8, byteorder="big", signed=False))
            if version_to_encode == 2:
                if msg.filter_value is not None:
                    fp_write(len(msg.filter_value).to_bytes(length=2, byteorder="big", signed=False))
                    fp_write(msg.filter_value.encode("utf-8"))

            data = msg.data
            fp_write(len(data).to_bytes(4, "big", signed=False))
            fp_write(data)

        fp.seek(0)
        length = fp.getbuffer().nbytes - 4
        fp_write(length.to_bytes(4, "big", signed=False))

        fp_write(frame.key.value.to_bytes(2, "big", signed=False))
        if version_to_encode > 1:
            version = version_to_encode
        else:
            version = frame.version
        fp_write(version.to_bytes(2, "big", signed=False))

        return fp.getvalue()


def _decode_field(buf: io.BytesIO, tp: Any, version_to_decode: int = 1) -> Any:
    if tp is T.string:
        length = int.from_bytes(buf.read(2), "big", signed=False)
        return buf.read(length).decode("utf-8")

    elif tp is T.bytes:
        length = int.from_bytes(buf.read(4), "big", signed=False)
        return buf.read(length)

    elif tp is T.raw:
        return buf.read()

    elif isinstance(tp, list):
        length = int.from_bytes(buf.read(4), "big", signed=False)
        result = []
        if len(tp) == 1:
            for _ in range(length):
                value = _decode_field(buf, tp[0], version_to_decode)
                result.append(value)
        else:
            for _ in range(length):
                row = []
                for subtype in tp:
                    value = _decode_field(buf, subtype, version_to_decode)
                    row.append(value)
                result.append(row)
        return result

    elif tp in int_specs:
        spec = int_specs[tp]
        return int.from_bytes(buf.read(spec.length), spec.byteorder, signed=spec.signed)

    elif is_struct(tp):
        return _decode_struct(buf, tp, version_to_decode)

    else:
        raise NotImplementedError(f"Unexpected type {tp}")


def _decode_struct(buf: io.BytesIO, tp: Type[Struct], version: int = 1) -> Struct:
    data = {}  # type: ignore
    fld_tp: Any
    for fld_name, fld_tp, type_ in tp.flds_meta:
        if fld_tp is None:
            if typing.get_origin(type_) is list:
                fld_tp = list(typing.get_args(type_))
            else:
                fld_tp = type_

        if version == 1 and fld_name == "filter_value":
            data[fld_name] = None

        else:
            data[fld_name] = _decode_field(buf, fld_tp, version)

    return tp(**data)  # type:ignore[call-arg]


def decode_frame(data: bytes, version_to_decode: int = 1) -> Frame:
    buf = io.BytesIO(data)
    key = int.from_bytes(buf.read(2), "big", signed=False)
    version = int.from_bytes(buf.read(2), "big", signed=False)

    is_response = key >> 15 & 1 == 1
    key &= ~(1 << 15)
    cls: Type[Frame] = registry[(is_response, Key(key))]
    if version != version_to_decode:
        raise ValueError(f"Version mismatch, got version: {version}")

    try:
        frame = _decode_struct(buf, cls, version_to_decode)
        assert (extra := buf.read()) == b"", f"Got extra bytes: {extra!r}"
    except Exception as e:
        raise ValueError(f"Could not decode {cls!r}") from e

    return cast(Frame, frame)
