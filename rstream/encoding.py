import io
import typing
from dataclasses import fields, is_dataclass
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
from .schema import Frame, Struct, registry

__all__ = ['encode_frame', 'decode_frame']


class IntSpec(NamedTuple):
    length: int
    byteorder: Literal['little', 'big']
    signed: bool


int_specs = {
    T.int8: IntSpec(length=1, byteorder='big', signed=True),
    T.int16: IntSpec(length=2, byteorder='big', signed=True),
    T.int32: IntSpec(length=4, byteorder='big', signed=True),
    T.int64: IntSpec(length=8, byteorder='big', signed=True),
    T.uint8: IntSpec(length=1, byteorder='big', signed=False),
    T.uint16: IntSpec(length=2, byteorder='big', signed=False),
    T.uint32: IntSpec(length=4, byteorder='big', signed=False),
    T.uint64: IntSpec(length=8, byteorder='big', signed=False),
}

_VT = Union[int, str, bytes, Struct]
VT = Annotated[Union[_VT, list[_VT]], 'Field value']

_TT = Union[T, Struct, None]
TT = Annotated[Union[_TT, list[_TT]], 'Field type metadata']


def _encode_field(value: VT, tp: TT) -> Union[bytearray, bytes]:
    if isinstance(tp, T) and tp in int_specs:
        assert isinstance(value, int)
        return value.to_bytes(**int_specs[tp]._asdict())

    elif tp is T.string:
        assert isinstance(value, str)
        buffer = bytearray()
        buffer += len(value).to_bytes(2, 'big', signed=False)
        buffer += value.encode('utf-8')
        return buffer

    elif tp is T.bytes:
        assert isinstance(value, bytes)
        buffer = bytearray()
        buffer += len(value).to_bytes(4, 'big', signed=False)
        buffer += value
        return buffer

    elif tp is T.raw:
        assert isinstance(value, bytes)
        return value

    elif is_dataclass(value):
        assert isinstance(value, Struct)
        return _encode_struct(value)

    elif isinstance(value, list):
        assert tp is None or isinstance(tp, list)
        buffer = bytearray()
        buffer += len(value).to_bytes(4, 'big', signed=False)
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

    else:
        raise NotImplementedError(f'Unexpected type {tp}, value: {value!r}')


def _encode_struct(struct: Struct) -> bytearray:
    buffer = bytearray()
    for fld in fields(struct):
        value = getattr(struct, fld.name)
        tp = fld.metadata.get('type')
        buffer += _encode_field(value, tp)
    return buffer


def encode_frame(frame: Frame) -> bytearray:
    try:
        payload = _encode_struct(frame)
    except Exception as e:
        raise ValueError(f'Could not encode frame {frame!r}') from e

    length = len(payload) + 2 + 2
    buffer = bytearray()
    buffer += length.to_bytes(4, 'big', signed=False)
    buffer += frame.key.value.to_bytes(2, 'big', signed=False)
    buffer += frame.version.to_bytes(2, 'big', signed=False)
    buffer += payload
    return buffer


def _decode_field(buf: io.BytesIO, tp: Any) -> Any:
    if tp is T.string:
        length = int.from_bytes(buf.read(2), 'big', signed=False)
        return buf.read(length).decode('utf-8')

    elif tp is T.bytes:
        length = int.from_bytes(buf.read(4), 'big', signed=False)
        return buf.read(length)

    elif tp is T.raw:
        return buf.read()

    elif isinstance(tp, list):
        length = int.from_bytes(buf.read(4), 'big', signed=False)
        result = []
        if len(tp) == 1:
            for _ in range(length):
                value = _decode_field(buf, tp[0])
                result.append(value)
        else:
            for _ in range(length):
                row = []
                for subtype in tp:
                    value = _decode_field(buf, subtype)
                    row.append(value)
                result.append(row)
        return result

    elif tp in int_specs:
        spec = int_specs[tp]
        return int.from_bytes(buf.read(spec.length), spec.byteorder, signed=spec.signed)

    elif is_dataclass(tp):
        return _decode_struct(buf, tp)

    else:
        raise NotImplementedError(f'Unexpected type {tp}')


def _decode_struct(buf: io.BytesIO, tp: Type[Struct]) -> Struct:
    data = {}
    for f in fields(tp):
        fld_tp = f.metadata.get('type')
        if fld_tp is None:
            if typing.get_origin(f.type) is list:
                fld_tp = list(typing.get_args(f.type))
            else:
                fld_tp = f.type

        data[f.name] = _decode_field(buf, fld_tp)

    return tp(**data)  # type:ignore[call-arg]


def decode_frame(data: bytes) -> Frame:
    buf = io.BytesIO(data)
    key = int.from_bytes(buf.read(2), 'big', signed=False)
    version = int.from_bytes(buf.read(2), 'big', signed=False)

    is_response = (key >> 15 & 1 == 1)
    key &= ~(1 << 15)
    cls: Type[Frame] = registry[(is_response, Key(key))]
    if version != cls.version:
        raise ValueError(f'Version mismatch, got version: {version}')

    try:
        frame = _decode_struct(buf, cls)
        assert (extra := buf.read()) == b'', f'Got extra bytes: {extra!r}'
    except Exception as e:
        raise ValueError(f'Could not decode {cls!r}') from e

    return cast(Frame, frame)
