import pytest
from datetime import datetime
from sentenai.stream.fields import Field, Fields

def test_values(client):
    assert type((client.streams['weather'] @ datetime.utcnow())['humidity']) == float


def test_fields(client):
    for x in client.streams['weather']:
        assert isinstance(x, Field)

def test_field_values(client):
    assert type(client.streams['weather']['humidity'] @ datetime.utcnow()) == float

def test_field_mean(client):
    assert type(client.streams['weather']['humidity'].mean) == float

def test_field_min(client):
    assert type(client.streams['weather']['humidity'].min) == float

def test_field_max(client):
    assert type(client.streams['weather']['humidity'].max) == float

def test_field_std(client):
    assert type(client.streams['weather']['humidity'].std) == float

def test_field_unique(client):
    u = client.streams['weather']['icon'].unique
    assert type(u) == dict


