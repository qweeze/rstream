# Copyright 2023 VMware, Inc. All Rights Reserved.
# SPDX-License-Identifier: MIT

from importlib import metadata

try:
    __version__ = metadata.version(__package__)
    __license__ = metadata.metadata(__package__)["license"]
except metadata.PackageNotFoundError:
    __version__ = "dev"
    __license__ = None

del metadata

from .amqp import AMQPMessage, amqp_decoder  # noqa: E402
from .compression import CompressionType  # noqa: E402
from .constants import OffsetType  # noqa: E402
from .constants import (  # noqa: E402
    ConsumerOffsetSpecification,
)
from .consumer import (  # noqa: E402
    Consumer,
    EventContext,
    MessageContext,
)
from .exceptions import OffsetNotFound  # noqa: E402
from .exceptions import ServerError  # noqa: E402
from .exceptions import StreamDoesNotExist  # noqa: E402
from .producer import (  # noqa: E402
    ConfirmationStatus,
    Producer,
    RawMessage,
)
from .schema import OffsetSpecification  # noqa: E402
from .superstream_consumer import (  # noqa: E402
    SuperStreamConsumer,
)
from .superstream_producer import (  # noqa: E402
    RouteType,
    SuperStreamProducer,
)

__all__ = [
    "AMQPMessage",
    "amqp_decoder",
    "Consumer",
    "RawMessage",
    "Producer",
    "OffsetType",
    "ConsumerOffsetSpecification",
    "ConfirmationStatus",
    "CompressionType",
    "SuperStreamProducer",
    "RouteType",
    "SuperStreamConsumer",
    "MessageContext",
    "ServerError",
    "OffsetNotFound",
    "StreamDoesNotExist",
    "OffsetSpecification",
    "EventContext",
]
