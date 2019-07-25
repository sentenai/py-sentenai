from sentenai.view import Views, View
import pytest


def test_views(client):
    assert isinstance(client.views, Views)


def test_view_create_and_get(client):
    v = client.views.create('foo', {
        'a': client.streams['weather']['temperatureMax']
    })

    assert client.views['foo'].name == v.name


def test_view_delete(client):
    del client.views['foo']
    with pytest.raises(KeyError):
        client.views['foo']


def test_view_anon(client):
    v = client.views(a=client.streams['weather']['temperatureMax'])
    assert isinstance(v, View)

@pytest.fixture(scope="module")
def vdata(client):
    v = client.views.create('test-weather', {"a": client.streams['weather']['temperatureMax']})
    return v.data

def test_view_data_limit(client, vdata):
    t0 = client.streams['weather'].events[0].ts
    t1 = client.streams['weather'].events[-1].ts
    x = vdata[t0:t1:10]
    assert isinstance(x, list) and len(x) == 10

def test_view_data_limit_0(client, vdata):
    t0 = client.streams['weather'].events[0].ts
    t1 = client.streams['weather'].events[-1].ts
    x = vdata[t0:t1:0]
    assert isinstance(x, list) and len(x) == 0

def test_view_data_limit_neg(client, vdata):
    t0 = client.streams['weather'].events[0].ts
    t1 = client.streams['weather'].events[-1].ts
    with pytest.raises(ValueError):
        x = vdata[t0:t1:-10]

