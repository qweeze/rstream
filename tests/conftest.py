import logging

import pytest

import rstream.client
from rstream import Consumer, Producer
from rstream.client import Client

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    handlers=[logging.StreamHandler()],
)


@pytest.fixture(scope='session', autouse=True)
def configure():
    rstream.client.DEFAULT_REQUEST_TIMEOUT = 1


def pytest_addoption(parser):
    parser.addoption('--rmq-host', action='store', default='localhost')
    parser.addoption('--rmq-port', action='store', default=5552)
    parser.addoption('--rmq-vhost', action='store', default='/')
    parser.addoption('--rmq-username', action='store', default='guest')
    parser.addoption('--rmq-password', action='store', default='guest')


@pytest.fixture()
async def no_auth_client(pytestconfig):
    rstream.client.DEFAULT_REQUEST_TIMEOUT = 1
    client = Client(
        host=pytestconfig.getoption('rmq_host'),
        port=pytestconfig.getoption('rmq_port'),
        frame_max=1024 * 1024,
        heartbeat=60,
    )
    await client.start()
    try:
        yield client
    finally:
        await client.close()


@pytest.fixture()
async def client(no_auth_client: Client, pytestconfig):
    await no_auth_client.authenticate(
        vhost=pytestconfig.getoption('rmq_vhost'),
        username=pytestconfig.getoption('rmq_username'),
        password=pytestconfig.getoption('rmq_password'),
    )
    return no_auth_client


@pytest.fixture()
async def stream(client: Client):
    await client.create_stream('test-stream')
    try:
        yield 'test-stream'
    finally:
        await client.delete_stream('test-stream')


@pytest.fixture()
async def consumer(pytestconfig):
    consumer = Consumer(
        host=pytestconfig.getoption('rmq_host'),
        port=pytestconfig.getoption('rmq_port'),
        username=pytestconfig.getoption('rmq_username'),
        password=pytestconfig.getoption('rmq_password'),
        frame_max=1024 * 1024,
        heartbeat=60,
    )
    await consumer.start()
    try:
        yield consumer
    finally:
        await consumer.close()


@pytest.fixture()
async def producer(pytestconfig):
    producer = Producer(
        host=pytestconfig.getoption('rmq_host'),
        port=pytestconfig.getoption('rmq_port'),
        username=pytestconfig.getoption('rmq_username'),
        password=pytestconfig.getoption('rmq_password'),
        frame_max=1024 * 1024,
        heartbeat=60,
    )
    await producer.start()
    try:
        yield producer
    finally:
        await producer.close()
