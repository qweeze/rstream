import enum


class T(enum.Enum):
    int8 = enum.auto()
    int16 = enum.auto()
    int32 = enum.auto()
    int64 = enum.auto()
    uint8 = enum.auto()
    uint16 = enum.auto()
    uint32 = enum.auto()
    uint64 = enum.auto()
    string = enum.auto()
    bytes = enum.auto()
    raw = enum.auto()


class Key(enum.Enum):
    DeclarePublisher = 1
    Publish = 2
    PublishConfirm = 3
    PublishError = 4
    QueryPublisherSequence = 5
    DeletePublisher = 6
    Subscribe = 7
    Deliver = 8
    Credit = 9
    StoreOffset = 10
    QueryOffset = 11
    Unsubscribe = 12
    Create = 13
    Delete = 14
    Metadata = 15
    MetadataUpdate = 16
    PeerProperties = 17
    SaslHandshake = 18
    SaslAuthenticate = 19
    Tune = 20
    Open = 21
    Close = 22
    Heartbeat = 23
    Route = 24
    Partitions = 25


class OffsetType(int, enum.Enum):
    first = 1
    last = 2
    next = 3
    offset = 4
    timestamp = 5
