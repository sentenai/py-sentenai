import pytest
from uuid import uuid4
from sentenai.stream import Stream

def test_streams_iter(client):
    """tests iterability as dict of Streams object."""
    assert dict(client.streams) or True


def test_streams_get(client):
    """Tests default getitem of Streams object."""
    n = uuid4().hex
    s = client.streams[n]
    assert isinstance(s, Stream)
    assert s.name == n
    assert bool(s) == False


def test_delete_nonexistent_stream(client):
    n = uuid4().hex
    with pytest.raises(KeyError):
        del client.streams[n]

