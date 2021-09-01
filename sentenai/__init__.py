from sentenai.api import API, Credentials, PANDAS
from sentenai.stream import Streams, Event
from sentenai.view import Views
from sentenai.pattern import Patterns
from sentenai.pattern.expression import InCircle
if PANDAS: import pandas as pd

from threading import Thread
import time
from queue import Queue, Empty

__all__ = ['Client', 'Event']

if PANDAS:
    def df(events):
        return pd.DataFrame([x.as_record() for x in events])



class Client(API):
    def __init__(self, host, auth_key=""):
        API.__init__(self, Credentials(host, auth_key))

    def __repr__(self):
        return "Client(auth_key=\"{}\", host=\"{}\")".format(self._credentials.auth_key, self._credentials.host)

    def ping(self):
        """float: Ping Sentenai cluster's health check and get back response time in seconds."""
        t0 = time.time()
        r = self._get()
        if r.status_code == 200:
            return time.time() - t0
        else:
            raise Exception(r.status_code)

    @property
    def streams(self):
        """Streams: the sub-api for streams."""
        return Streams(self)

    @property
    def views(self):
        """Views: the sub-api for views."""
        return Views(self)

    @property
    def patterns(self):
        """Patterns: the sub-api for patterns."""
        return Patterns(self)







