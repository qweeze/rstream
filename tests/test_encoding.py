import pytest

from rstream import OffsetType, schema
from rstream.encoding import (
    decode_frame,
    encode_frame,
)

frames = [
    schema.PeerProperties(correlation_id=1, properties=[schema.Property(key='product', value='rmq-streams-client')]),
    schema.PeerPropertiesResponse(
        correlation_id=1, response_code=1,
        properties=[schema.Property(key='cluster_name', value='rabbit@f333b2024c8e')],
    ),
    schema.SaslHandshake(correlation_id=2),
    schema.SaslHandshakeResponse(correlation_id=2, response_code=1, mechanisms=['PLAIN', 'AMQPLAIN']),
    schema.SaslAuthenticate(correlation_id=3, mechanism='PLAIN', data=b'\x00guest\x00guest'),
    schema.SaslAuthenticateResponse(correlation_id=3, response_code=1),
    schema.Tune(frame_max=1048576, heartbeat=60),
    schema.Open(correlation_id=4, virtual_host='/'),
    schema.Heartbeat(),
    schema.OpenResponse(
        correlation_id=4, response_code=1,
        properties=[schema.Property(key='advertised_port', value='5552')],
    ),
    schema.Metadata(correlation_id=5, streams=['mystream']),
    schema.MetadataResponse(
        correlation_id=5,
        brokers=[schema.Broker(reference=0, host='localhost', port=5552)],
        metadata=[schema.StreamMetadata(name='mystream', response_code=1, leader_ref=0, replicas_refs=[])],
    ),
    schema.DeclarePublisher(correlation_id=7, publisher_id=1, reference='mystream_publisher_1', stream='mystream'),
    schema.DeclarePublisherResponse(correlation_id=7, response_code=1),
    schema.QueryPublisherSequence(correlation_id=8, publisher_ref='mystream_publisher_1', stream='mystream'),
    schema.QueryPublisherSequenceResponse(correlation_id=8, response_code=1, sequence=293),
    schema.Publish(publisher_id=1, messages=[schema.Message(publishing_id=294, data=b'hello')]),
    schema.PublishConfirm(publisher_id=1, publishing_ids=[294]),
    schema.DeletePublisher(correlation_id=9, publisher_id=1),
    schema.DeletePublisherResponse(correlation_id=9, response_code=1),
    schema.Close(correlation_id=10, code=1, reason='OK'),
    schema.CloseResponse(correlation_id=10, response_code=1),
    schema.QueryOffset(correlation_id=6, reference='mystream_subscriber_1', stream='mystream'),
    schema.QueryOffsetResponse(correlation_id=6, response_code=1, offset=239),
    schema.Subscribe(
        correlation_id=7, subscription_id=1, stream='mystream',
        offset_spec=schema.OffsetSpecification(offset_type=OffsetType.offset, offset=412),
        credit=10, properties=[],
    ),
    schema.SubscribeResponse(correlation_id=7, response_code=1),
    schema.Deliver(
        subscription_id=1, magic_version=80, chunk_type=0, num_entries=1, num_records=1,
        timestamp=1628717042903, epoch=1, chunk_first_offset=239, chunk_crc=3486439553, data_length=10,
        trailer_length=30, _reserved=0, data=b'\x00\x00\x00\x06msg: 9',
    ),
    schema.Credit(subscription_id=1, credit=1),
    schema.Unsubscribe(correlation_id=8, subscription_id=1),
    schema.UnsubscribeResponse(correlation_id=8, response_code=1),
    schema.StoreOffset(reference='mystream_subscriber_1', stream='mystream', offset=240),
]

is_response = {frame_cls: is_response for (is_response, _), frame_cls in schema.registry.items()}


@pytest.mark.parametrize('frame', frames)
def test_encoding(frame: schema.Frame) -> None:
    raw = bytearray(encode_frame(frame))
    del raw[:4]  # get rid of length header

    if is_response[frame.__class__]:
        # if a frame is a response, first bit of a key must be switched to 1
        key = int.from_bytes(raw[:2], 'big', signed=False) | (1 << 15)
        raw[:2] = key.to_bytes(2, 'big', signed=False)

    if frame.__class__ is schema.Subscribe:
        pytest.skip('TODO: fix issue with offset_spec')

    assert decode_frame(raw) == frame
