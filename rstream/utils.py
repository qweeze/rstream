# Copyright 2023 VMware, Inc. All Rights Reserved.
# SPDX-License-Identifier: MIT

import abc
import asyncio
import itertools
from dataclasses import dataclass
from typing import Any, Generator, Optional, Callable, Union, Awaitable

from .amqp import _MessageProtocol, AMQPMessage


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
class DisconnectionErrorInfo:
    reason: str
    streams: list[str]


'''
class FilterConfiguration(abc.ABC):

    @abc.abstractmethod
    def values(self, filter_values: []) -> None:
        pass

    @abc.abstractmethod
    def post_filler(self, predicate: Callable[[AMQPMessage], Union[None, Awaitable[None]]]) -> None:
        pass

    @abc.abstractmethod
    def match_unfiltered(self) -> None:
        pass

    @abc.abstractmethod
    def match_unfiltered_set(self, match_unfiltered: bool) -> None:
        pass

'''


class FilterConfiguration:

    def __init__(self, values_to_filter: [], predicate: Optional[Callable[[AMQPMessage], Union[None, Awaitable[None]]]], match_unfiltered: bool = False):
        self._values_to_filter = values_to_filter
        self._predicate = predicate
        self._match_unfiltered = match_unfiltered


'''
    def values(self, filter_values: []) -> FilterConfiguration:
        pass

    def post_filler(self, predicate: Callable[[AMQPMessage], Union[None, Awaitable[None]]]) -> None:
        pass

    def match_unfiltered(self) -> None:
        pass

    def match_unfiltered_set(self, match_unfiltered: bool) -> None:
        pass
'''