from importlib import metadata

__version__ = metadata.version(__package__)
__license__ = metadata.metadata(__package__)["license"]
del metadata


from .amqp import AMQPMessage, amqp_decoder
from .constants import OffsetType
from .consumer import Consumer
from .producer import Producer, RawMessage

__all__ = [
    "AMQPMessage",
    "amqp_decoder",
    "Consumer",
    "RawMessage",
    "Producer",
    "OffsetType",
]
