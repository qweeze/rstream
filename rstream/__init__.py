from importlib import metadata

try:
    __version__ = metadata.version(__package__)
    __license__ = metadata.metadata(__package__)["license"]
except metadata.PackageNotFoundError:
    __version__ = "dev"
    __license__ = None

del metadata

from .amqp import AMQPMessage, amqp_decoder  # noqa: E402
from .constants import OffsetType  # noqa: E402
from .consumer import Consumer  # noqa: E402
from .producer import Producer, RawMessage  # noqa: E402

__all__ = [
    "AMQPMessage",
    "amqp_decoder",
    "Consumer",
    "RawMessage",
    "Producer",
    "OffsetType",
]
