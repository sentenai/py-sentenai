from sentenai.stream.metadata import Metadata
from sentenai.stream.events import Updates
from sentenai.api import *
if PANDAS:
    import pandas as pd
from datetime import datetime
import simplejson as JSON
import re, io
from collections import namedtuple


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
                j['ts'] = dt64(data.start)
            if data.end:
                j['duration'] = int((dt64(data.end) - dt64(j['ts'])).astype('timedelta64[ns]'))
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


class Updates(API):
    def __init__(self, parent):
        API.__init__(self, parent._credentials, *parent._prefix, "events")
        self._log = None
        self._parent = parent

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
                elif item.stop is not None:
                    hdrs['start'] = iso8601(item.start)
                    hdrs["end"] = iso8601(item.stop)
                if item.step is None:
                    r = self._post(headers=hdrs, json=data)
                else:
                    r = self._put(item.step, headers=hdrs, json=data)
                if 300 > r.status_code >= 200:
                    return None
                else:
                    raise Exception(r.status_code)
        else:
            raise ValueError("Must be a slice of the form `start : end* : id*`, where `*` indicates optional)")

    def __enter__(self):
        self._log = Log(self._parent)
        return self._log

    def __exit__(self, x, y, z):
        self._log._queue.close()
        self._log._thread.join()
        self._log = None

    def __delitem__(self, item):
        if isinstance(item, slice):
            raise TypeError("slice not supported for deleting updates.")
        else:
            r = self._delete(item)
            if r.status_code == 204:
                return None
            else:
                raise Exception(r.status_code)


    def __getitem__(self, item):
        if isinstance(item, slice):
            params = {}
            if item.start is not None:
                params['start'] = iso8601(item.start)
            if item.stop is not None:
                params['end'] = iso8601(item.stop)
            if item.step is not None:
                params['limit'] = abs(item.step)
                if item.step < 0:
                    params['sort'] = 'desc'
            r = self._get(params=params)
            if r.status_code == 200:
                data = []
                for line in r.json():
                    data.append({
                        'start': dt64(line['ts']),
                        'end': dt64(line['ts']) + td64(line['duration']) if line['duration'] else None,
                        'data': line['event'],
                    })
                return data
            else:
                raise Exception(r.json())
        else:
            r = self._get(item)
            if r.status_code == 404:
                raise KeyError(f"Update with id `{item}` not found.")
            else:
                return r.json()




class Streams(API):
    def __init__(self, parent, name):
        self._parent = parent
        self._name = name
        self._origin = None
        API.__init__(self, parent._credentials, *parent._prefix, "streams", name)

    @property
    def log(self):
        return Updates(self)

    def __len__(self):
        return int(self._head('events').headers['events'])

    def __iter__(self):
        r = self._get("fields")
        if r.status_code != 200:
            raise sentenaierror("invalid response")
        data = r.json()
        return iter(sorted([f['path'][0] for f in data if len(f['path']) == 1]))

    def keys(self):
        return iter(self)
   
    def items(self):
        return iter([(k, self[k]) for k in self])

    def values(self):
        return iter([self[k] for k in self])

    @property
    def graph(self):
        import treelib
        t = treelib.Tree()
        root = t.create_node(self.name, self.name, data={})
        r = self._get("fields")
        if r.status_code != 200:
            raise SentenaiError("Invalid Response")
        data = r.json()
        for node in sorted(data, key=lambda x: x['path']):
            parent = root
            x = [self._name]
            for link in node['path']:
                x.append(link)
                nid = "/".join(x)
                if nid in t:
                    continue
                else:
                    pid = "/".join(x[:-1]) 
                    t.create_node(link, nid, parent=pid, data={'type': node['type']})
        return t


    @property
    def meta(self):
        return Metadata(self)

    def load(self, file):
        self._post(file)

    def __getitem__(self, key):
        if isinstance(key, tuple):
            return Stream(self, *key)
        else:
            return Stream(self, key)

    def __repr__(self):
        return f"Streams({self._parent!r}, \"{self._name}\")"

    def __str__(self):
        return str(self._name)

    @property
    def origin(self):
        try:
            self._origin = dt64(self._head().headers.get('origin'))
            return self._origin
        except TypeError:
            self._origin = None

    @property
    def name(self):
        return self._name




        

class Stream(API):
    def __init__(self, parent, *path):
        self._parent = parent
        self._path = path
        API.__init__(self, parent._credentials, *parent._prefix)

    def __iter__(self):
        r = self._get("fields")
        if r.status_code != 200:
            raise SentenaiError("invalid response")
        data = r.json()
        pl = len(self._path)
        return iter(sorted(f[pl] for f in data if self._path == tuple(f[:pl]) and len(f) == pl + 1))

    @property
    def graph(self):
        return self._parent.graph.subtree(str(self))

    @property
    def meta(self):
        return Metadata(self)

    @property
    def type(self):
        r = self._get('type', *self._path)
        if r.status_code == 200:
            return r.json()['type']
        else:
            return None

    @property
    def range(self):
        r = self._get('range', *self._path)
        if r.status_code == 200:
            e = r.json()
            return (e['start'], e['end'])
        else:
            return None


    def __repr__(self):
        z = ", ".join(map(repr, self._path))
        return f"Stream({self._parent!r}, {z})"

    def __str__(self):
        return "/".join((self._parent.name,) + self._path)

    def __getitem__(self, key):
        if isinstance(key, tuple):
            return Stream(self._parent, *(self._path + key))
        else:
            return Stream(self._parent, *(self._path + (key,)))
   
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

    def __len__(self):
        return StreamStats(self, vtype="event", ro=True).count
    
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

        ps = {'origin': iso8601(o or self._parent._parent.origin)}
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














