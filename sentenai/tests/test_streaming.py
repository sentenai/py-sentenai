from sentenai.stream.events import Event
from datetime import datetime
import pytest, time
from uuid import uuid4
import ndjson
import requests
import pdb


def test_streaming(client):


    response = requests.get("http://127.0.0.1:3333/streams/buffalo/events")
    items = response.json(cls=ndjson.Decoder)

    for i in items:
        print(i)
