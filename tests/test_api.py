from hypothesis import given, example, assume
from hypothesis.strategies import text, tuples, uuids, one_of, none, integers, floats, datetimes

from sentenai import Sentenai
from sentenai.api.stream import Stream
import string, unittest, requests_mock, requests, pytest

try:
    from urllib.parse import quote
except:
    from urllib import quote

def stream(name, *filters):
    return Stream(None, name, {}, None, True, *filters)

URL = "https://api.sentenai.com/"
URL_STREAMS   = URL + "streams"
URL_STREAM_ID = URL + "streams/{}"
URL_EVENTS    = URL + "streams/{}/events"
URL_EVENTS_ID = URL + "streams/{}/events/{}"
# TODO: this is changing
URL_STREAM_ID_META = URL + "streams/{}"

test_client = Sentenai(auth_key = "")

def test_streams_call():
    with requests_mock.mock() as m:
        m.get(URL_STREAMS, json=[])
        resp = test_client.streams()
        assume(list(resp) == [])

def test_stream_existence():
    with requests_mock.mock() as m:
        s = test_client.Stream('real-stream')
        m.get(URL_STREAM_ID.format(s.name), status_code=200)
        assume(bool(s) == True)

def test_stream_nonexistence():
    with requests_mock.mock() as m:
        s = test_client.Stream('fake-stream')
        m.get(URL_STREAM_ID.format(s.name), status_code=404)
        assume(bool(s) == False)

def test_stream_len():
    with requests_mock.mock() as m:
        s = test_client.Stream('weather')
        count = 8675309
        m.get(URL_STREAM_ID_META.format(s.name), json={ 'events': count })
        assume(len(s) == count)

def test_stream_stats():
    with requests_mock.mock() as m:
        s = test_client.Stream('weather')
        payload = { 'events': 3, 'fields': [['some'], ['fields']], 'healthy': True, 'meta': {} }
        m.get(URL_STREAM_ID_META.format(s.name), json=payload)
        assume(s.stats() == payload)

@given(text())
@example(None)
def test_delete_sid_typechecks(sid):
    with pytest.raises(TypeError):
        test_client.delete(sid, "")

@given(text(min_size=1))
def test_delete_with_eid(eid):
    s = stream("foo")

    def mock_encodings(m, quoter):
        try:
            m.delete(URL_EVENTS_ID.format(s.name, quoter(eid)))
        except:
            pass

    with requests_mock.mock() as m:
        mock_encodings(m, lambda x: x)
        mock_encodings(m, quote)
        mock_encodings(m, lambda x: quote(x.encode('utf-8', 'ignore')))

        assume(test_client.delete(s, eid) == None)


