from hypothesis import given, example, assume
from hypothesis.strategies import text, tuples, uuids

from sentenai.api import (Sentenai)
import string, unittest, requests_mock, requests

test_client = Sentenai(auth_key = "")

class HappyApiTests(unittest.TestCase):
    @given(text(min_size=1), uuids())
    def test_delete_with_eid(self, stream_name, eid):
        with requests_mock.mock() as m:
            m.delete("https://api.senten.ai/streams/" + stream_name + "/events/" + str(eid), text="foo")
            #resp = test_client.delete(stream_name, str(eid))
            pass

    @given(text(min_size=1))
    def test_delete_no_eid(self, stream_name):
        with requests_mock.mock() as m:
            m.delete("https://api.senten.ai/streams/" + stream_name + "/events", text="foo")
            #resp = test_client.delete(stream_name)
            pass


class SadApiTests(unittest.TestCase):
    @given(text(min_size=1), uuids())
    def test_delete_with_eid(self, stream_name, eid):
        with requests_mock.mock() as m:
            m.delete("https://api.senten.ai/streams/" + stream_name + "/events/" + str(eid), text="foo")
            pass

    @given(text(min_size=1))
    def test_delete_no_eid(self, stream_name):
        with requests_mock.mock() as m:
            m.delete("https://api.senten.ai/streams/" + stream_name + "/events", text="foo")
            pass


