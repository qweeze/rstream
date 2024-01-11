# Copyright 2023 VMware, Inc. All Rights Reserved.
# SPDX-License-Identifier: MIT

import asyncio
import itertools
from dataclasses import dataclass
from typing import Any, Callable, Generator, Optional

from .amqp import AMQPMessage, _MessageProtocol


@dataclass
class RawMessage(_MessageProtocol):
    data: bytes
    publishing_id: Optional[int] = None

    def __bytes__(self) -> bytes:
        return self.data


class MonotonicSeq:
    def __init__(self) -> None:
        self._seq = itertools.count(1)

    def next(self) -> int:
        return next(self._seq)

    def set(self, value: int) -> None:
        self._seq = itertools.count(value)

    def reset(self) -> None:
        self.set(1)


class TimeoutWrapper:
    def __init__(self, future: asyncio.Future[Any], timeout: int) -> None:
        self.future = future
        self.timeout = timeout

    async def _wait(self) -> Any:
        return await asyncio.wait_for(self.future, self.timeout)

    def __await__(self) -> Generator[Any, None, Any]:
        return self._wait().__await__()


@dataclass
class OnClosedErrorInfo:
    reason: str
    streams: list[str]


class FilterConfiguration:
    def __init__(
        self,
        values_to_filter: list[str],
        predicate: Optional[Callable[[AMQPMessage], bool]] = None,
        match_unfiltered: bool = False,
    ):
        self._values_to_filter = values_to_filter
        self._predicate = predicate
        self._match_unfiltered = match_unfiltered

    def values(self) -> list[str]:
        return self._values_to_filter

    def post_filler(self) -> Optional[Callable[[AMQPMessage], bool]]:
        return self._predicate

    def match_unfiltered(self) -> bool:
        return self._match_unfiltered
