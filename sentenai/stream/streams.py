from sentenai.stream.metadata import Metadata
from sentenai.stream.events import Events, Event
from sentenai.stream.fields import Fields, Field
from sentenai.api import *
if PANDAS:
    import pandas as pd
from datetime import datetime
import simplejson as JSON
import re, io
from collections import namedtuple

# Optional for parquet handling
try:
    import pyarrow
except:
    pass

from queue import Queue
from threading import Thread

Update = namedtuple('Update', ['id', 'start', 'end', 'data'])

class WQueue(Queue):
    def write(self, data):
        if data:
            j = {}
            if data.id:
                j['id'] = id
            if not data.start:
                j['ts'] = dt64(datetime.utcnow())
            else:
                j['ts'] = data.start
            if data.end:
                j['duration'] = data.end - j['ts']
            j['event'] = data.data
            self.put((JSON.dumps(j, ignore_nan=True, cls=SentenaiEncoder) + "\n").encode('utf-8'))

    def __iter__(self):
        return iter(self.get, None)

    def gen(self):
        while True:
            x = self.get()
            if x is None:
                break
            else:
                yield x


    def close(self):
        self.put(None)

class Log(Thread):
    def __init__(self, parent):
        self._queue = WQueue(1000)
        self._parent = parent
        self._thread = Thread(target=self._post)
        self._thread.start()

    def _post(self):
        self._parent._post(json=self._queue.gen())

    def __setitem__(self, item, data):
        if isinstance(item, slice):
            if item.start is None:
                raise ValueError("start time must be specified")
            elif item.stop and item.start >= item.stop:
                raise ValueError("end time must be after start time")
            else:
                self._queue.write(Update(start=item.start, end=item.stop, id=item.step, data=data))
        else:
            raise ValueError("Must be a slice of the form `start : end* : id*`, where `*` indicates optional)")


class Streams(API):
    def __init__(self, parent, name):
        self._parent = parent
        self._name = name
        self._origin = None
        API.__init__(self, parent._credentials, *parent._prefix, "streams", name)
        self._log = None


    def __enter__(self):
        self._log = Log(self)
        return self._log

    def __exit__(self, x, y, z):
        self._log._queue.close()
        self._log._thread.join()
        self._log = None



    def __getitem__(self, key):
        if isinstance(key, tuple):
            return Stream(self, *key)
        else:
            return Stream(self, key)

    def __setitem__(self, item, data):
        hdrs = {'content-type': 'application/json'}
        if isinstance(item, slice):
            if item.start is None:
                raise ValueError("start time must be specified")
            elif item.stop and item.start >= item.stop:
                raise ValueError("end time must be after start time")
            else:
                if item.start is not None and item.stop is None:
                    hdrs["timestamp"] = iso8601(item.start)
                elif evt.stop is not None:
                    hdrs['start'] = iso8601(item.start)
                    hdrs["end"] = iso8601(item.stop)
                if item.step is None:
                    r = self._post("events", headers=hdrs, json=data)
                else:
                    r = self._put("events", item.step, headers=hdrs, json=data)
                if 300 > r.status_code >= 200:
                    return None
                else:
                    raise Exception(r.status_code)
        else:
            raise ValueError("Must be a slice of the form `start : end* : id*`, where `*` indicates optional)")

    def __repr__(self):
        return f"Streams({self._parent!r}, \"{self._name}\")"

    def __str__(self):
        return str(self._name)

    @property
    def origin(self):
        if self._origin:
            return self._origin
        else:
            return self._head().headers.get('t0')

    @property
    def name(self):
        return self._name

    @property
    def streams(self):
        r = self._get("fields")
        if r.status_code != 200:
            raise SentenaiError("invalid response")
        data = r.json()
        return [Stream(self, *x) for x in sorted([f['path'] for f in data])]



        

class Stream(API):
    def __init__(self, parent, *path):
        self._parent = parent
        self._path = path
        API.__init__(self, parent._credentials, *parent._prefix)

    @property
    def type(self):
        return self._get('type', *self._path).json()['type']

    def __repr__(self):
        z = ", ".join(map(repr, self._path))
        return f"Stream({self._parent!r}, {z})"

    def __str__(self):
        return "/".join((self._parent.name,) + self._path)

    def __getitem__(self, key):
        if isinstance(key, tuple):
            return Stream(self._parent, self._path + key)
        else:
            return Stream(self._parent, self._path + (key,))
   
    @property
    def data(self):
        return StreamData(self)

    @property
    def first(self):
        try:
            return self.data[::1][0]
        except KeyError:
            return None

    @property
    def head(self, n=20):
        if n <= 0: return None
        return self.data[::n]
    
    @property
    def tail(self, n=20):
        if n <= 0: return None
        return self.data[::-n]

    @property
    def last(self):
        try:
            return self.data[::-1][0]
        except KeyError:
            return None
    
class StreamData(object):
    def __init__(self, parent):
        self._parent = parent

    def __getitem__(self, tr):
        if isinstance(tr, tuple):
            if len(tr) == 2:
                s, o = tr
                t = self._parent.type
            elif len(tr) == 3:
                s, o, t = tr
            else:
                raise ValueError("invalid arguments to .data")
        else:
            s = tr
            o = None
            t = self._parent.type

        ps = {'t0': self._parent._parent.origin if o is None else iso8601(o)}
        if s.start is not None:
            ps['start'] = iso8601(s.start)
        if s.stop is not None:
            ps['end'] = iso8601(s.stop)
        if s.step is not None:
            ps['limit'] = int(s.step)
        
        r = self._parent._get('data', "/".join(self._parent._path) + "." + t, params=ps)
        data = r.json()
        if isinstance(data, list):
            for evt in data:
                evt['start'] = dt64(evt['start'])
                evt['end'] = dt64(evt['end'])
                if t != 'event':
                    evt['value'] = fromJSON(t, evt['value'])
            return data
        else:
            raise Exception(data)














