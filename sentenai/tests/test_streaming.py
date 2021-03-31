from sentenai.stream.events import Event
from datetime import datetime
import pytest, time
from uuid import uuid4
import ndjson
import requests
import pdb
import json

def test_streaming(client):

    # get json
    headers = {'Accept': 'application/json'}
    response = requests.get("http://127.0.0.1:3333/streams/hartford/events", headers=headers)
    items = response.json()

    len1 = len(items)

    #post nd json
    with open('hartford.ndjson') as f:
        input = ndjson.load(f)

    dataSize = len(input)
    headers = {'Content-type': 'application/x-ndjson'}
    r = requests.post('http://127.0.0.1:3333/streams/hartford', data=ndjson.dumps(input), headers=headers)
    assert r.status_code == 201

    time.sleep(5) # allow riak to persist
    # get x-ndjson
    headers = {'Accept': 'application/x-ndjson'}
    response = requests.get("http://127.0.0.1:3333/streams/hartford/events", headers=headers)
    output = response.json(cls=ndjson.Decoder)
    len2 = len(output)

    assert all(item in input for item in output)



   # TODO compare actual response contents to file content. should match exactly
    # post json
    with open('single.json') as f:
        data = json.load(f)

    headers = {'Content-type': 'application/json'}
    r = requests.post('http://127.0.0.1:3333/streams/hartford/events', json=data, headers=headers)
    assert r.status_code == 201

    time.sleep(5) # allow riak to persist
    # get json
    headers = {'Accept': 'application/json'}
    response = requests.get("http://127.0.0.1:3333/streams/hartford/events", headers=headers)
    items = response.json()

    len1 = len(items)

    assert len1 == len2 +1
