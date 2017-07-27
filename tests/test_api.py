from hypothesis import given, example, assume
from hypothesis.strategies import text, tuples, uuids, one_of, none, integers, floats, datetimes

from sentenai import Sentenai, stream
import string, unittest, requests_mock, requests, pytest

try:
    from urllib.parse import quote
except:
    from urllib import quote



URL = "https://api.senten.ai/"
URL_STREAMS   = URL + "streams"
URL_STREAM_ID = URL + "streams/{}"
URL_EVENTS    = URL + "streams/{}/events"
URL_EVENTS_ID = URL + "streams/{}/events/{}"

test_client = Sentenai(auth_key = "")

def test_streams_call():
    with requests_mock.mock() as m:
        m.get(URL_STREAMS, json=[])
        resp = test_client.streams()
        assume(resp == [])

def test_query_call():
    with requests_mock.mock() as m:
        m.get(URL_STREAMS, json=[])
        resp = test_client.streams()
        assume(resp == [])


@given(text())
@example(None)
def test_delete_sid_typechecks(sid):
    with pytest.raises(TypeError):
        test_client.delete(sid, "")

@given(one_of(integers(), floats(), datetimes()))
@example(None)
@example("")
def test_delete_eid_typechecks(eid):
    with pytest.raises(TypeError):
        test_client.delete(stream("foo"), eid)


@given(text(min_size=1))
def test_delete_with_eid(eid):
    s = stream("foo")

    def mock_encodings(m, quoter):
        try:
            m.delete(URL_EVENTS_ID.format(s['name'], quoter(eid)))
        except:
            pass

    with requests_mock.mock() as m:
        mock_encodings(m, lambda x: x)
        mock_encodings(m, quote)
        mock_encodings(m, lambda x: quote(x.encode('utf-8', 'ignore')))

        assume(test_client.delete(s, eid) == None)


