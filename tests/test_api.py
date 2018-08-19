from hypothesis import given, example, assume
from hypothesis.strategies import text, tuples, uuids, one_of, none, integers, floats, datetimes

from sentenai import Sentenai
from sentenai.api.stream import Stream
from sentenai.utils import iso8601
import string, unittest, requests_mock, requests, pytest
from datetime import datetime

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
URL_VALUES    = URL + "streams/{}/values"
# TODO: this is changing
URL_STREAM_ID_META = URL + "streams/{}"
URL_STREAM_RANGE = URL + "streams/{}/start/{}/end/{}"

def range_url(stream, start, end):
    return URL_STREAM_RANGE.format(stream.name, iso8601(start), iso8601(end))

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

def test_stream_healthy():
    with requests_mock.mock() as m:
        s = test_client.Stream('weather')
        m.get(URL_STREAM_ID_META.format(s.name), json={ 'healthy': True })
        assume(s.healthy() == True)

def test_stream_healthy_404():
    with requests_mock.mock() as m:
        s = test_client.Stream('weather')
        m.get(URL_STREAM_ID_META.format(s.name), status_code=404)
        assume(s.healthy() == None)

def test_empty_range():
    with requests_mock.mock() as m:
        s = test_client.Stream('weather')

        start = datetime(2000,1,1)
        end = datetime(2000,1,2)
        m.get(range_url(s, start, end), text='')

        results = s.range(start, end).df()
        assume(len(results) == 0)

def test_empty_select():
    with requests_mock.mock() as m:
        s = test_client.Stream('weather')
        loc = '1234'

        m.post("/query", headers={ 'location': loc })
        m.get("/query/" + loc + "/spans", json={ 'spans': [] })

        results = test_client.select(s.temp > 1000).df()
        assume(len(results) == 0)

def test_stream_values():
    with requests_mock.mock() as m:
        s = test_client.Stream('weather')

        when = datetime(2018,1,1,3,55)
        url_when = "2018-01-01T03%3A55%3A00%2B00%3A00"

        m.get(URL_VALUES.format(s.name) + '?at=' + url_when,
            json=[
                {'ts': '2017-12-04T04:00:00Z', 'path': ['temp'], 'value': 37.47, 'id': '123'},
                {'ts': '2017-12-05T04:00:00Z', 'path': ['nested', 'humidity'], 'value': 0.75, 'id': 'abc'},
            ]
            )
        items = s.values(at=when).items()

        assert len(items) == 2
        assert str(items[0][0]) == '(stream "weather"):temp'
        assert str(items[1][0]) == '(stream "weather"):nested.humidity'
        assert items[0][1] == 37.47
        assert items[1][1] == 0.75

def test_create_event():
    with requests_mock.mock() as m:
        s = test_client.Stream('weather')

        evt = s.Event(id='55', data={ 'temp': 32 }, ts=datetime(2018,1,1,3,55))
        assume(evt.exists == False)

        m.put(URL_EVENTS_ID.format(s.name, evt.id))
        evt.create()
        assume(evt.exists == True)

def test_read_event():
    with requests_mock.mock() as m:
        s = test_client.Stream('weather')

        evt = s.Event(id='55')

        m.get(URL_EVENTS_ID.format(s.name, evt.id),
            headers={
                'location': evt.id,
                'timestamp': '2011-07-21 18:00:00'
                },
            json={
                'temp': 32
            }
            )
        evt.read()
        assume(evt.exists == True)
        assume(evt.json()['event']['temp'] == 32)

def test_delete_event():
    with requests_mock.mock() as m:
        s = test_client.Stream('weather')
        evt = s.Event(id='55', data={ 'temp': 32 }, ts=datetime(2018,1,1,3,55), saved=True)

        m.delete(URL_EVENTS_ID.format(s.name, evt.id))
        evt.delete()
        assume(evt.exists == False)

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


