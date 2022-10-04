from sentenai.api import *
from sentenai.stream import Database
if PANDAS: import pandas as pd
from datetime import datetime

import time

__all__ = ['Sentenai']

if PANDAS:
    def df(events):
        return pd.DataFrame([x.as_record() for x in events])

class Sentenai(API):
    def __init__(self, host=None, port=None, protocol=None, check=True):
        ## We do this so we can programmatically pass in host/port
        if host is None:
            host = 'localhost'
        if port is None:
            port = 7280
        if protocol is None:
            protocol = 'http://'

        h = f"{protocol}{host}:{port}"
        API.__init__(self, Credentials(h, ""))
        if check and self.ping() > 0.5:
            print("warning: connection to this repository may be high latency or unstable.")

    def __repr__(self):
        return "Sentenai(host=\"{}\")".format(self._credentials.host)

    def __call__(self, tspl):
        return View(self, tspl)

    def __iter__(self):
        r = self._get('db')
        return iter(sorted(s for s in r.json()))

    def keys(self):
        return iter(self)
   
    def items(self):
        return iter([(k, self[k]) for k in self])

    def values(self):
        return iter([self[k] for k in self])

    def init(self, name, origin=datetime(1970,1,1,0,0)):
        """Initialize a new stream database. The origin is the earliest
        point in time storable in the database. Data may not come before that time.
        If `None` is provided, then all updates must be logged with integer timestamps
        representing nanoseconds since the origin `0`.
        """
        if origin == None:
            r = self._put("db", name)
        else:
            r = self._put("db", name, json={'origin': iso8601(origin)})
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
        x = self._get('db', db)
        if x.status_code != 200:
            raise KeyError(f"`{db}` not found in {self!r}.")
        return Database(self, db, dt64(x.json().get('origin')))

    
    if PANDAS:
        def df(self, tspl):
            """Dataframe"""
            return View(self, tspl, True)



class View(API):
    def __init__(self, parent, tspl, df=False):
        self._parent = parent
        API.__init__(self, parent._credentials, *parent._prefix, "tspl")
        self._tspl = tspl
        self._df = df
        self._info = None
    def __repr__(self):
        return self._tspl
    
    def explain(self):
        return self._post("debug", json=self._tspl).json()

    @property
    def range(self):
        if self._info:
            return {'start': dt64(self._info['start']), 'end': dt64(self._info['end'])}
        else:
            self._info = self._post("range", json=self._tspl).json()
            return self.range

    @property
    def origin(self):
        if self._info:
            return dt64(self._info.get('origin'))
        else:
            self._info = self._post("range", json=self._tspl).json()
            return self.origin

    @property
    def type(self):
        if self._info:
            return self._info.get('type')
        else:
            self._info = self._post("range", json=self._tspl).json()
            return self.type




    def __getitem__(self, i):
        params = {}
        if isinstance(i, tuple):
            i, o = isinstance

        if isinstance(i, slice):
            #params['sort'] = 'asc'

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
                params['limit'] = i.step
                #if i.step < 0:
                #    params['sort'] = 'desc'

            resp = self._post(json=self._tspl, params=params)
                
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
    


