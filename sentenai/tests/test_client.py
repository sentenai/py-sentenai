from sentenai.api import API
from sentenai.stream import Streams


def test_ping(client):
    assert client.ping() > 0.

def test_streams(client):
    assert isinstance(client.streams, Streams) and isinstance(client.streams, API)

