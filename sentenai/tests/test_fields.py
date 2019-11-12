from sentenai.stream.events import Event
import pytest, time
from datetime import datetime, timedelta


@pytest.fixture(scope='module')
def test_stream(client, request):
    if client.streams['test-stats']:
        del client.streams['test-stats']
    client.streams['test-stats'].init()
    time.sleep(1)
    def teardown():
        del client.streams['test-stats']
    for i in range(20):
        client.streams['test-stats'].events.insert(Event(id=str(i), ts=datetime(2010,1,1,0,0,i), data={"i": i}))
    time.sleep(10)
    request.addfinalizer(teardown)
    return client.streams['test-stats']

def test_field_value(client, test_stream):
    x = test_stream['i'].value()
    assert x == 19

def test_field_value_at(client, test_stream):
    ts = test_stream.events[5].ts
    x = test_stream['i'].value(at=ts)
    assert x == 5

def test_field_value_at_operator(client, test_stream):
    ts = test_stream.events[5].ts
    assert test_stream['i'].value(at=ts) == test_stream['i'] @ ts

def test_mean(client, test_stream):
    assert test_stream['i'].mean == 9.5

def test_min(client, test_stream):
    assert test_stream['i'].min == 0

def test_max(client, test_stream):
    assert test_stream['i'].max == 19

def test_count(client, test_stream):
    assert test_stream['i'].count == 20
