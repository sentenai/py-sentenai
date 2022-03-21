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

    def __iter__(self):
        r = self._get("fields")
        if r.status_code != 200:
            raise SentenaiError("invalid response")
        data = r.json()
        return iter([Stream(self, *x) for x in sorted([f['path'] for f in data])])


    def load(self, file):
        self._post(file)

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
        if not self._origin:
            self._origin = dt64(self._head().headers.get('t0'))
        return self._origin

    @property
    def name(self):
        return self._name




        

class Stream(API):
    def __init__(self, parent, *path):
        self._parent = parent
        self._path = path
        API.__init__(self, parent._credentials, *parent._prefix)

    @property
    def type(self):
        r = self._get('type', *self._path)
        if r.status_code == 200:
            return r.json()['type']
        else:
            return None

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
    @property
    def stats(self):
        return StreamStats(self)

    
class StreamStats(object):
    def __init__(self, parent, start=None, end=None, origin=None, vtype=None, ro=False):
        self._parent = parent
        self._start = start
        self._end = end
        self._type = vtype
        self._origin = origin
        self._read_only = ro

    def __getitem__(self, tr):
        if self._read_only:
            raise TypeError("Stream stats already has a time range")

        if isinstance(tr, tuple):
            if len(tr) == 2:
                s, o = tr
                t = self._type
            elif len(tr) == 3:
                s, o, t = tr
            else:
                raise ValueError("invalid arguments to .data")
            o = iso8601(o)
        else:
            s = tr
            o = self._origin
            t = self._type

        if s.start is not None:
            self._start = s.start
        if s.stop is not None:
            self._end = s.stop

        self._read_only = True
        return self


    def _stat(self, stat):
        origin = self._origin or self._parent._parent.origin or dt64("1970-01-01T00:00:00")
        start = self._start or origin
        end = self._end or origin + td64(2 ** 63 - 1)
        try:
            return self._parent._parent._parent(f"""
                {stat}({self._parent})
                    when interval({iso8601(start)}, {iso8601(end)})
                    origin {iso8601(origin)}
            """)[::1][0]['value']
        except BadRequest:
            raise TypeError from None
        except IndexError:
            return None
        except KeyError:
            return None

    @property
    def count(self):
        return self._stat('count')

    @property
    def mean(self):
        return self._stat('mean')

    @property
    def min(self):
        return self._stat('min')

    @property
    def max(self):
        return self._stat('max')

    @property
    def sum(self):
        return self._stat('sum')

    @property
    def std(self):
        return self._stat('std')

    @property
    def top(self):
        return self._stat('top')

    @property
    def any(self):
        return self._stat('any')

    @property
    def all(self):
        return self._stat('all')

    
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

        ps = {'t0': iso8601(o or self._parent._parent.origin)}
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














