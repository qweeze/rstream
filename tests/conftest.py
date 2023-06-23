import logging
import ssl

import pytest

import rstream.client
from rstream import (
    Consumer,
    Producer,
    RouteType,
    SuperStreamConsumer,
    SuperStreamProducer,
)
from rstream.client import Client

from .http_requests import (
    create_binding,
    create_exchange,
    delete_exchange,
)
from .util import (
    routing_extractor,
    routing_extractor_for_sac,
    routing_extractor_key,
)

logging.basicConfig(
    level=logging.WARNING,
    format="[%(asctime)s] %(levelname)s: %(message)s",
    handlers=[logging.StreamHandler()],
)


@pytest.fixture(scope="session", autouse=True)
def configure():
    rstream.client.DEFAULT_REQUEST_TIMEOUT = 1


def pytest_addoption(parser):
    parser.addoption("--rmq-host", action="store", default="localhost")
    parser.addoption("--rmq-port", action="store", default=5552)
    parser.addoption("--rmq-ssl", action="store", type=bool, default=False)
    parser.addoption("--rmq-vhost", action="store", default="/")
    parser.addoption("--rmq-username", action="store", default="guest")
    parser.addoption("--rmq-password", action="store", default="guest")


@pytest.fixture()
def ssl_context(pytestconfig):
    if pytestconfig.getoption("rmq_ssl"):
        return ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    else:
        return None


@pytest.fixture()
async def no_auth_client(pytestconfig, ssl_context):
    rstream.client.DEFAULT_REQUEST_TIMEOUT = 1
    client = Client(
        host=pytestconfig.getoption("rmq_host"),
        port=pytestconfig.getoption("rmq_port"),
        frame_max=1024 * 1024,
        heartbeat=60,
        ssl_context=ssl_context,
    )
    await client.start()
    try:
        yield client
    finally:
        await client.close()


@pytest.fixture()
async def client(no_auth_client: Client, pytestconfig):
    await no_auth_client.authenticate(
        vhost=pytestconfig.getoption("rmq_vhost"),
        username=pytestconfig.getoption("rmq_username"),
        password=pytestconfig.getoption("rmq_password"),
    )
    return no_auth_client


@pytest.fixture()
async def stream(client: Client):
    await client.create_stream("test-stream")
    try:
        yield "test-stream"
    finally:
        await client.delete_stream("test-stream")


@pytest.fixture()
async def consumer(pytestconfig, ssl_context):
    consumer = Consumer(
        host=pytestconfig.getoption("rmq_host"),
        port=pytestconfig.getoption("rmq_port"),
        ssl_context=ssl_context,
        username=pytestconfig.getoption("rmq_username"),
        password=pytestconfig.getoption("rmq_password"),
        frame_max=1024 * 1024,
        heartbeat=60,
    )
    await consumer.start()
    try:
        yield consumer
    finally:
        await consumer.close()


@pytest.fixture()
async def producer(pytestconfig, ssl_context):
    producer = Producer(
        host=pytestconfig.getoption("rmq_host"),
        port=pytestconfig.getoption("rmq_port"),
        ssl_context=ssl_context,
        username=pytestconfig.getoption("rmq_username"),
        password=pytestconfig.getoption("rmq_password"),
        frame_max=1024 * 1024,
        heartbeat=60,
    )
    await producer.start()
    try:
        yield producer
    finally:
        await producer.close()


@pytest.fixture()
async def super_stream(client: Client):
    # create an exchange to connect the 3 supersteams
    super_stream = "test-super-stream"
    status_code = create_exchange(exchange_name=super_stream)
    assert status_code == 201 or status_code == 204

    await client.create_stream(super_stream + "-0")
    await client.create_stream(super_stream + "-1")
    await client.create_stream(super_stream + "-2")

    # create binding with exchange
    status_code = create_binding(
        exchange_name=super_stream, routing_key="key1", stream_name=super_stream + "-0"
    )
    assert status_code == 201 or status_code == 204
    status_code = create_binding(
        exchange_name=super_stream, routing_key="key2", stream_name=super_stream + "-1"
    )
    assert status_code == 201 or status_code == 204
    status_code = create_binding(
        exchange_name=super_stream, routing_key="key3", stream_name=super_stream + "-2"
    )
    assert status_code == 201 or status_code == 204

    try:
        yield "test-super-stream"
    #
    finally:
        await client.delete_stream(super_stream + "-0")
        await client.delete_stream(super_stream + "-1")
        await client.delete_stream(super_stream + "-2")

        status_code = delete_exchange(exchange_name=super_stream)
        assert status_code == 201 or status_code == 204


@pytest.fixture()
async def super_stream_producer(pytestconfig, ssl_context):
    producer = SuperStreamProducer(
        host=pytestconfig.getoption("rmq_host"),
        port=pytestconfig.getoption("rmq_port"),
        ssl_context=ssl_context,
        username=pytestconfig.getoption("rmq_username"),
        password=pytestconfig.getoption("rmq_password"),
        frame_max=1024 * 1024,
        heartbeat=60,
        routing=RouteType.Hash,
        routing_extractor=routing_extractor,
        super_stream="test-super-stream",
    )
    await producer.start()
    try:
        yield producer
    finally:
        await producer.close()


@pytest.fixture()
async def super_stream_key_routing_producer(pytestconfig, ssl_context):
    producer = SuperStreamProducer(
        host=pytestconfig.getoption("rmq_host"),
        port=pytestconfig.getoption("rmq_port"),
        ssl_context=ssl_context,
        username=pytestconfig.getoption("rmq_username"),
        password=pytestconfig.getoption("rmq_password"),
        frame_max=1024 * 1024,
        heartbeat=60,
        routing=RouteType.Key,
        routing_extractor=routing_extractor_key,
        super_stream="test-super-stream",
    )
    await producer.start()
    try:
        yield producer
    finally:
        await producer.close()


@pytest.fixture()
async def super_stream_producer_for_sac(pytestconfig, ssl_context):
    producer = SuperStreamProducer(
        host=pytestconfig.getoption("rmq_host"),
        port=pytestconfig.getoption("rmq_port"),
        ssl_context=ssl_context,
        username=pytestconfig.getoption("rmq_username"),
        password=pytestconfig.getoption("rmq_password"),
        frame_max=1024 * 1024,
        heartbeat=60,
        routing=RouteType.Hash,
        routing_extractor=routing_extractor_for_sac,
        super_stream="test-super-stream",
    )
    await producer.start()
    try:
        yield producer
    finally:
        await producer.close()


@pytest.fixture()
async def super_stream_consumer(pytestconfig, ssl_context):
    consumer = SuperStreamConsumer(
        host=pytestconfig.getoption("rmq_host"),
        port=pytestconfig.getoption("rmq_port"),
        ssl_context=ssl_context,
        username=pytestconfig.getoption("rmq_username"),
        password=pytestconfig.getoption("rmq_password"),
        frame_max=1024 * 1024,
        heartbeat=60,
        super_stream="test-super-stream",
    )
    await consumer.start()
    try:
        yield consumer
    finally:
        await consumer.close()


@pytest.fixture()
async def super_stream_consumer_for_sac1(pytestconfig, ssl_context):

    consumer = SuperStreamConsumer(
        host=pytestconfig.getoption("rmq_host"),
        port=pytestconfig.getoption("rmq_port"),
        ssl_context=ssl_context,
        username=pytestconfig.getoption("rmq_username"),
        password=pytestconfig.getoption("rmq_password"),
        frame_max=1024 * 1024,
        heartbeat=60,
        super_stream="test-super-stream",
    )
    await consumer.start()
    try:
        yield consumer
    finally:
        await consumer.close()


@pytest.fixture()
async def super_stream_consumer_for_sac2(pytestconfig, ssl_context):
    consumer = SuperStreamConsumer(
        host=pytestconfig.getoption("rmq_host"),
        port=pytestconfig.getoption("rmq_port"),
        ssl_context=ssl_context,
        username=pytestconfig.getoption("rmq_username"),
        password=pytestconfig.getoption("rmq_password"),
        frame_max=1024 * 1024,
        heartbeat=60,
        super_stream="test-super-stream",
    )
    await consumer.start()
    try:
        yield consumer
    finally:
        await consumer.close()


@pytest.fixture()
async def super_stream_consumer_for_sac3(pytestconfig, ssl_context):

    consumer = SuperStreamConsumer(
        host=pytestconfig.getoption("rmq_host"),
        port=pytestconfig.getoption("rmq_port"),
        ssl_context=ssl_context,
        username=pytestconfig.getoption("rmq_username"),
        password=pytestconfig.getoption("rmq_password"),
        frame_max=1024 * 1024,
        heartbeat=60,
        super_stream="test-super-stream",
    )
    await consumer.start()
    try:
        yield consumer
    finally:
        await consumer.close()


@pytest.fixture()
async def super_stream_consumer_for_sac4(pytestconfig, ssl_context):

    consumer = SuperStreamConsumer(
        host=pytestconfig.getoption("rmq_host"),
        port=pytestconfig.getoption("rmq_port"),
        ssl_context=ssl_context,
        username=pytestconfig.getoption("rmq_username"),
        password=pytestconfig.getoption("rmq_password"),
        frame_max=1024 * 1024,
        heartbeat=60,
        super_stream="test-super-stream",
    )
    await consumer.start()
    try:
        yield consumer
    finally:
        await consumer.close()
