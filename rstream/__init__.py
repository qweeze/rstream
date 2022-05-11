from importlib import metadata

from .amqp import AMQPMessage, amqp_decoder
from .constants import OffsetType
from .consumer import Consumer
from .producer import Producer, RawMessage

try:
    __version__ = metadata.version(__package__)
    __license__ = metadata.metadata(__package__)["license"]
except metadata.PackageNotFoundError:
    __version__ = "dev"
    __license__ = None

del metadata

__all__ = [
    "AMQPMessage",
    "amqp_decoder",
    "Consumer",
    "RawMessage",
    "Producer",
    "OffsetType",
]
