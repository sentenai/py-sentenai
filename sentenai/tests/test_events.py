from sentenai.stream.events import Event
from datetime import datetime
import pytest, time

def test_create_event(client):
    e = client.streams['foo'].events.insert(Event(ts=datetime.utcnow(), data={'foo': 'bar'}))
    assert isinstance(e, Event)

def test_get_event(client):
    e = client.streams['foo'].events.insert(Event(ts=datetime.utcnow(), data={'foo': 'bar'}))
    assert e == client.streams['foo'].events[e.id]

def test_remove_event(client):
    foo_evts = client.streams['foo'].events
    e = foo_evts.insert(Event(ts=datetime.utcnow(), data={'foo': 'bar'}))
    foo_evts.remove(e)
    with pytest.raises(KeyError):
        foo_evts[e.id]

def test_remove_non_existent_event(client):
    e = Event(id="a", ts=datetime.utcnow(), data={'foo': 'bar'})
    with pytest.raises(KeyError):
        client.streams['foo'].events.remove(e)


def test_update_event(client):
    foo_evts = client.streams['foo'].events
    e = foo_evts.insert(Event(ts=datetime.utcnow(), data={'foo': 'bar'}))
    e.data = {"bar": "baz"}
    foo_evts.update(e)
    assert foo_evts[e.id].data == {"bar": "baz"}


@pytest.fixture(scope='module')
def evts_stream(client, request):
    if client.streams['test-events']:
        del client.streams['test-events']
    def teardown():
        for i in range(20):
            del client.streams['test-events'].events[str(i)]
        del client.streams['test-events']
    for i in range(20):
        client.streams['test-events'].events.insert(Event(id=str(i), ts=datetime(2015,1,1+i), data={"i": i}))
    request.addfinalizer(teardown)
    time.sleep(5)
    return client.streams['test-events']


def test_events_length(client, evts_stream):
    assert len(evts_stream.events) == 20

def test_events_range_int(client, evts_stream):
    x = evts_stream.events[:10]
    assert len(x) == 10

def test_events_range_int_vals(client, evts_stream):
    x = evts_stream.events[:10]
    for i in range(10):
        assert i == x[i].data['i']

def test_events_newest(client, evts_stream):
    x = evts_stream.events[-1]
    assert x.data['i'] == 19

def test_events_oldest(client, evts_stream):
    x = evts_stream.events[0]
    assert x.data['i'] == 0

def test_events_third_oldest(client, evts_stream):
    x = evts_stream.events[2]
    assert x.data['i'] == 2

def test_events_third_newest(client, evts_stream):
    x = evts_stream.events[-3]
    assert x.data['i'] == 17

def test_events_range_datetimeleft(client, evts_stream):
    x = evts_stream.events[datetime(2015,1,1):]
    assert len(x) == 20
    for i, e in zip(range(25), x):
        assert i == x[i].data['i']

def test_events_range_datetime_right(client, evts_stream):
    x = evts_stream.events[:datetime(2015,1,5)]
    assert len(x) == 4
    for i, e in zip(range(25), x):
        assert i == x[i].data['i']

def test_events_range_datetime_full(client, evts_stream):
    x = evts_stream.events[datetime(2015,1,1):datetime(2015,1,21)]
    assert len(x) == 20
    for i, e in zip(range(25), x):
        assert i == x[i].data['i']

def test_events_range_datetime_full2(client, evts_stream):
    x = evts_stream.events[datetime(2015,1,1):datetime(2015,1,21)]
    assert len(x) == 20
    for i, e in zip(range(25), x):
        assert i == x[i].data['i']

def test_events_range_full_limit(client, evts_stream):
    x = evts_stream.events[datetime(2015,1,3):datetime(2015,1,15):5]
    assert len(x) == 5
    for i, e in zip(range(25), x):
        assert i + 2 == x[i].data['i']

def test_events_range_full(client, evts_stream):
    x = evts_stream.events[:]
    assert len(x) == 20
    for i, e in zip(range(25), x):
        assert i == x[i].data['i']
