from sentenai.api import API, Credentials, PANDAS, iso8601, dt64
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

    def __call__(self, tspl):
        return View(self, tspl)

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
    
    if PANDAS:
        def df(self, tspl):
            return View(self, tspl, True)



class View(API):
    def __init__(self, parent, tspl, df=False):
        self._parent = parent
        API.__init__(self, parent._credentials, *parent._prefix, "patterns")
        self._tspl = tspl
        self._df = df

    def __repr__(self):
        return self._tspl
    
    def explain(self):
        return self._post("umbra/plan", json=f'{self._tspl}').json()

    def __getitem__(self, i):
        params = {}

        if isinstance(i, slice):
            params['sort'] = 'asc'

            if i.start is None:
                pass
            elif type(i.start) is int:
                params['start'] = int(i.start)
            else:
                params['start'] = iso8601(i.start)

            if i.stop is None:
                pass
            elif type(i.stop) is int:
                params['end'] = int(i.stop)
            else:
                params['end'] = iso8601(i.stop)

            if i.step is not None:
                params['limit'] = abs(i.step)
                if i.step < 0:
                    params['sort'] = 'desc'

            data = self._post("umbra/exec", json=f'{self._tspl}', params=params).json()
            if isinstance(data, list):
                for evt in data:
                    evt['start'] = dt64(evt['start'])
                    evt['end'] = dt64(evt['end'])
                if self._df:
                    return pd.DataFrame(data)
                else:
                    return data

            else:
                print(data)
                raise Exception(data)


        else:
            raise Exception("wrong type")
    


