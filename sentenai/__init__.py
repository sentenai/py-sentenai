from sentenai.api import *
from sentenai.stream import Streams, Event
from sentenai.view import Views
from sentenai.pattern import Patterns
from sentenai.pattern.expression import InCircle
if PANDAS: import pandas as pd
from datetime import datetime

from threading import Thread
import time
from queue import Queue, Empty

__all__ = ['Client', 'Event']

if PANDAS:
    def df(events):
        return pd.DataFrame([x.as_record() for x in events])

class Sentenai(API):
    def __init__(self, host="http://localhost:3333"):
        API.__init__(self, Credentials(host, ""))
        if self.ping() > 0.5:
            print("warning: connection to this repository may be high latency or unstable.")

    def __repr__(self):
        return "Sentenai(host=\"{}\")".format(self._credentials.host)

    def __call__(self, tspl):
        return View(self, tspl)

    def __iter__(self):
        r = self._get('streams')

        return iter(s['name'] for s in r.json())
            

    def init(self, name, origin=datetime(1970,1,1,0,0)):
        """Initialize a new stream database. The origin is the earliest
        point in time storable in the database. Data may not come before that time.
        If `None` is provided, then all updates must be logged with integer timestamps
        representing nanoseconds since the origin `0`.
        """
        if origin == None:
            r = self._put(name)
        else:
            r = self._put("streams", name, headers={'t0': iso8601(origin)}, json=None)
        if r.status_code != 201:
            raise Exception("Could not initialize")
        return self[name]

    def ping(self):
        """Ping Sentenai get back response time in seconds."""
        t0 = time.time()
        r = self._get()
        return time.time() - t0

    def __delitem__(self, name):
        """Delete a stream database"""
        self[name]._delete()

    def __getitem__(self, db):
        """Get a stream database."""
        x = self._head('streams', db)
        if x.status_code != 200:
            raise KeyError(f"`{db}` not found in {self!r}.")
        return Streams(self, db)
    
    if PANDAS:
        def df(self, tspl):
            """Dataframe"""
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
        if isinstance(i, tuple):
            i, o = isinstance

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

            resp = self._post("umbra/exec", json=f'{self._tspl}', params=params)
                
            t = resp.headers['type']
            data = resp.json()
            if isinstance(data, list):
                for evt in data:
                    evt['start'] = dt64(evt['start'])
                    evt['end'] = dt64(evt['end'])
                    if t != "event":
                        evt['value'] = fromJSON(t, evt['value'])
                    if self._df:
                        evt['duration'] = evt['end'] - evt['start']
                if self._df:
                    if t == "event":
                        return pd.DataFrame(data, columns=["start", "end", "duration"])
                    else:
                        return pd.DataFrame(data, columns=["start", "end", "duration", "value"])
                else:
                    return data

            else:
                print(data)
                raise Exception(data)


        else:
            raise Exception("wrong type")
    


