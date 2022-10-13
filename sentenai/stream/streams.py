from sentenai.stream.metadata import Metadata
from sentenai.stream.events import Updates
from sentenai.api import *
if PANDAS:
    import pandas as pd
from datetime import datetime
import simplejson as JSON
import re, io
from collections import namedtuple
from multiprocessing import Pool
from tqdm import tqdm, tqdm_notebook
from time import sleep

from queue import Queue
from threading import Thread

Update = namedtuple('Update', ['id', 'start', 'end', 'data'])

def index_data(args):
    db, node, index, v = args
    counter = 0
    while counter < 10:
        try:
            resp = db._post('nodes', node, 'types', index, json=v)
        except:
            counter += 1
            sleep(.1)
        else:
            break
    if resp.status_code > 204:
        raise Exception(f"failed on row {i}, column {k}")

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

class Logger(Thread):

    def join(self, timeout=None):
        super(Logger, self).join(timeout)
        if self._exc: raise self._exc

    def run(self):
        self._exc = None
        try:
            self.ret = self._target(*self._args, **self._kwargs)
        except BaseException as e:
            self._exc = e

class Log(object):
    def __init__(self, parent):
        self._queue = WQueue(1000)
        self._parent = parent
        self._thread = Logger(target=self._post)
        self._thread.start()
        self._exc = None

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


class Database(API):
    def __init__(self, parent, name, origin):
        self._parent = parent
        self._name = name
        self._origin = origin
        API.__init__(self, parent._credentials, *parent._prefix, "db", name)


    @property
    def log(self):
        raise Exception("disabled")
        return Updates(self)

    def __iter__(self):
        r = self._get("links")
        if r.status_code != 200:
            raise SentenaiError("invalid response")
        data = r.json()
        return iter(sorted(data.keys()))

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
        r = self._get("graph")
        if r.status_code != 200:
            raise SentenaiError("Invalid Response")
        data = r.json()
        for node in sorted(data, key=lambda x: x[0]):
            parent = root
            x = [self._name]
            for link in node[0]:
                x.append(link)
                nid = "/".join(x)
                if nid in t:
                    continue
                else:
                    pid = "/".join(x[:-1]) 
                    t.create_node(link, nid, parent=pid, data={'type': node[1]})
        return t


    @property
    def meta(self):
        raise NotImplemented("metadata not implemented on databases.")
        return Metadata(self)

    #def load(self, file):
    #    self._post(file)

    def __getitem__(self, key):
        if isinstance(key, tuple):
            return Stream(self, *key)
        else:
            return Stream(self, key)

    def __setitem__(self, key, df):
        del self[key]
        path = key if isinstance(key, tuple) else (key,)
        nid = self._put('paths', *path).json()['node']
        self._put('nodes', nid, 'types', 'event')
        df = df.sort_values(by='start', ignore_index=True)
        cmap = {'start': nid}
        tmap = {'start': 'event'}
        dmap = {'start': []}
        for cname in df.columns:
            if cname in ('start', 'end'):
                continue
            nid = self._put('paths', *path, cname).json()['node']
            if df[cname].dtype == np.dtype('float32'):
                tmap[cname] = 'float'
            elif df[cname].dtype == np.dtype('float64'):
                tmap[cname] = 'float'
            elif df[cname].dtype == np.dtype('int32'):
                tmap[cname] = 'int'
            elif df[cname].dtype == np.dtype('int64'):
                tmap[cname] = 'int'
            elif df[cname].dtype == bool:
                tmap[cname] = 'bool'
            elif df[cname].dtype == np.dtype('datetime64[ns]'):
                tmap[cname] = 'datetime'
            elif df[cname].dtype == np.dtype('timedelta64[ns]'):
                tmap[cname] = 'timedelta'
            else:
                tmap[cname] = 'text'

            self._put('nodes', nid, 'types', tmap[cname])
            cmap[cname] = nid
            dmap[cname] = []

        origin = self.origin
        for i, row in tqdm(df.iterrows(), total=len(df), unit='values', unit_scale=len(df.columns) - 1):
            if origin is not None:
                ts = (row['start'] - origin).delta
            else:
                ts = row['start']
            try:
                if 'end' in row and origin is not None:
                    dur = (row['end'] - row['start']).delta
                elif origin is not None:
                    dur = (df['start'].iloc[i+1] - row['start']).delta
                else:
                    raise Exception("origin should have been detected.")
                #elif 'end' in row:
                #    dur = df['end'] - df['start']
                #else:
                #    dur = (df['start'].iloc[i+1] - df['start']) / np.timedelta64(1, 'ns')
            except IndexError:
                dur = 1
            else:
                if dur <= 0: continue
                for col, val in dict(row).items():
                    if col == 'start':
                        dmap[col].append({'ts': ts, 'duration': dur})
                    else:
                        dmap[col].append({'ts': ts, 'duration': dur, 'value': val})

            if len(dmap['start']) >= 1024:
                with Pool(processes=16) as pool:
                    pool.map(index_data, [(self, cmap[k], tmap[k], dmap[k]) for k, v in dmap.items()])
                for k, v in dmap.items():
                    dmap[k] = []
                


    def __delitem__(self, key):
        if isinstance(key, tuple):
            self._delete('paths', *key)
        else:
            self._delete('paths', key)

    def __repr__(self):
        return f"Database({self._parent!r}, \"{self._name}\")"

    def __str__(self):
        return str(self._name)

    @property
    def origin(self):
        return self._origin

    @property
    def name(self):
        return self._name

class Stream(API):
    def __init__(self, parent, *path):
        self._parent = parent
        self._path = path
        r = self._parent._get('paths', *path)
        if r.status_code == 404:
            raise KeyError("path does not exist")
        else:
            self._node = r.json()['node']
        API.__init__(self, parent._credentials, *parent._prefix, "nodes", self._node)

    def __iter__(self):
        r = self._get("links")
        if r.status_code != 200:
            raise SentenaiError("invalid response")
        data = r.json()
        return iter(sorted(data.keys()))

    @property
    def graph(self):
        return self._parent.graph.subtree(str(self))

    @property
    def meta(self):
        return Metadata(self)

    @property
    def type(self):
        r = self._get('types')
        if r.status_code == 200:
            return r.json()[0]
        else:
            return None

    @property
    def range(self):
        if self.type is None:
            return None
        r = self._get('types', self.type, 'range')
        if r.status_code == 200:
            e = r.json()
            if self._parent.origin is None:
                return (e['start'], e['end'])
            else:
                return (self._parent.origin + np.timedelta64(e['start'], 'ns'), self._parent.origin + np.timedelta64(e['end'], 'ns'))
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
        return StreamData(self, self.type, False)

    @property
    def df(self):
        return StreamData(self, self.type, True)

    @property
    def first(self):
        try:
            return self.data[::1][0]
        except KeyError:
            return None

    def head(self, n=20, df=True):
        if n <= 0: return None
        if df:
            return self.df[::n]
        else:
            return self.data[::n]
    
    def tail(self, n=20, df=True):
        if n <= 0: return None
        if df:
            return self.df[::-n]
        else:
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
        raise NotImplemented
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
        start, end = self._parent.range

        #start = self._start or origin
        #end = self._end or origin + td64(2 ** 63 - 1)
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

    
class StreamData(API):
    def __init__(self, parent, index, df=False, resample=None, rolling=None):
        self._parent = parent
        self._df = df
        self._type = index
        self._resample = resample
        self._rolling = rolling
        API.__init__(self, parent._credentials, *parent._prefix, "types", index)

    def resample(self, period, aggregator=None):
        if self._type == 'event':
            aggregator = "count"
        return StreamData(self._parent, self._type, self._df, (period, aggregator), self._rolling)

    def rolling(self, period):
        return StreamData(self._parent, self._type, self._df, self._resample, (period, "trailing"))


    def __getitem__(self, tr):
        if self._rolling:
            r = f'window {self._rolling[0]} {self._rolling[1]}'
        else:
            r = ''
        if self._resample:
            if self._resample[1]:
                rs = f'{self._resample[1]}({self._parent!s}) when frequency({self._resample[0]}) {r}'
            else:
                rs = f'{self._parent!s} when frequency({self._resample[0]}) {r}'
        else:
            rs = f'{"when " if self._type == "event" else ""}{self._parent!s} {r}'

        if self._df:
            return self._parent._parent._parent.df(rs)[tr]
        else:
            return self._parent._parent._parent(rs)[tr]


