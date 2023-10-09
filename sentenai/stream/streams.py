from sentenai.stream.metadata import Metadata
from sentenai.api import *
if PANDAS:
    import pandas as pd
from datetime import datetime, time, date
import simplejson as JSON
import re, io, math
from collections import namedtuple
from multiprocessing import Pool
from tqdm import tqdm, tqdm_notebook
from time import sleep
import cbor2
from shapely.geometry import Point

from queue import Queue
from threading import Thread
from concurrent.futures import ThreadPoolExecutor



def worker(q, workers, total, progress, position=None):
    with ThreadPoolExecutor(max_workers=workers) as pool:
        if progress:
            v = 0
            with tqdm(total=total, unit=" values", position=position) as pbar:
                while True:
                    data = q.get()
                    if not data:
                        pbar.update(total - v)
                        break # exit on empty list
                    list(pool.map(index_data, data))
                    n = sum([len(v) for d, n, i, v in data])
                    v += n
                    pbar.update(n)
        else:
            while True:
                data = q.get()
                if not data:
                    break # exit on empty list
                list(pool.map(index_data, data))


def index_data(args):
    db, node, index, v = args
    counter = 0
    while counter < 10:
        try:
            data = cbor2.dumps(v)
        except Exception as e:
            print(e)
            print(db, node, index)
            print(v[-1])

        try:
            resp = db._post('nodes', node, 'types', index,
                    json=data, headers={'Content-Type': 'application/cbor'}, raw=True)
        except:
            counter += 1
            sleep(.1)
        else:
            if resp.status_code > 204:
                resp.close()
                raise Exception(f"failed on index")
            resp.close()
            break


class Database(API):
    def __init__(self, parent, name, origin):
        self._parent = parent
        self._name = name
        self._origin = origin
        API.__init__(self, parent._credentials, *parent._prefix, "db", name)



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

    def graph(self, path=None, limit=-1):
        import treelib
        t = treelib.Tree()
        root = t.create_node(path[-1] if path else self.name, path[-1] if path else self.name, data={})
        ps = {}
        if limit >= 0:
            ps['limit'] = limit
        r = self._get("graph", *(path or []), params=ps)
        if r.status_code != 200:
            raise SentenaiError("Invalid Response")
        data = r.json()
        for node in sorted(data, key=lambda x: x[0]):
            parent = root
            x = [path[-1] if path else self.name]
            for link in node[0][len(path) - 2 if path else 0:]:
                x.append(link)
                nid = "/".join(x)
                if nid in t:
                    continue
                else:
                    pid = "/".join(x[:-1]) 
                    t.create_node(link, nid, parent=pid, data={'id': node[1], 'type': node[2], 'children': node[3], 'indexes': node[4]})
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

    def __setitem__(self, key, content):
        workers = 32
        chunksize = 4096
        if isinstance(key, tuple):
            if isinstance(key[-1], slice):
                path = key[:-1]
                path += key[-1].start
                workers = key[-1].stop
                chunksize = key.step or chunksize
            else:
                path = key
        elif isinstance(key, slice):
            workers = key.stop
            chunksize = key.step or chunksize
            path = (key.start,)
        else:
            path = (key,)

        del self[path]

        if content is None:
            nid = self._put('paths', *path, json={'kind': 'directory'})
            return
        elif PANDAS and isinstance(content, pd.DataFrame):
            nid = self._put('paths', *path).json()['node']
            self._put('nodes', nid, 'types', 'event')
            cmap = {'start': nid}
            tmap = {'start': 'event'}
            dmap = {'start': []}
            df = content.sort_values(by='start', ignore_index=True)
        elif type(content) == str:
            nid = self._put('paths', *path, json={'kind': 'virtual', 'tspl': content})
            return
        elif content == float:
            nid = self._put('paths', *path).json()['node']
            self._put('nodes', nid, 'types', 'float')
            return
        elif content == int:
            nid = self._put('paths', *path).json()['node']
            self._put('nodes', nid, 'types', 'int')
            return
        elif content == bool:
            nid = self._put('paths', *path).json()['node']
            self._put('nodes', nid, 'types', 'bool')
            return
        elif content == datetime:
            nid = self._put('paths', *path).json()['node']
            self._put('nodes', nid, 'types', 'datetime')
            return
        elif content == date:
            nid = self._put('paths', *path).json()['node']
            self._put('nodes', nid, 'types', 'date')
            return
        elif content == time:
            nid = self._put('paths', *path).json()['node']
            self._put('nodes', nid, 'types', 'time')
            return
        elif content == str:
            nid = self._put('paths', *path).json()['node']
            self._put('nodes', nid, 'types', 'text')
            return
        elif isinstance(content, Stream):
            nid = content._node
            if isinstance(key, tuple) and len(key) > 1:
                src = self[key[:-1]]
                self._put('nodes', src._node, 'links', key[-1], nid)
            else:
                if isinstance(key, tuple) and len(key) > 1:
                    key = key[0]
                self._put('links', key, nid)
            return
        elif isinstance(content, list):
                cmap = {}
                tmap = {}
                dmap = {}
                df = pd.DataFrame(content).rename(columns={'value': path[-1]})
                if set(df.columns) != {'start', 'end', path[-1]}:
                    raise Exception(str(df.columns))
                df = df.sort_values(by='start', ignore_index=True)
        else:
            raise TypeError("invalid assignment type")

        if len(df) == 0:
            raise ValueError("Cannot index empty dataset")

        def add(cname):
            retries = 100
            while retries > 0:
                try:
                    if isinstance(content, list):
                        nid = self._put('paths', *path).json()['node']
                    else:
                        nid = self._put('paths', *path, cname).json()['node']
                    if df[cname].dtype == np.dtype('float32'):
                        tm = 'float'
                    elif df[cname].dtype == np.dtype('float64'):
                        tm = 'float'
                    elif df[cname].dtype == np.dtype('int32'):
                        tm = 'int'
                    elif df[cname].dtype == np.dtype('int64'):
                        tm = 'int'
                    elif df[cname].dtype == bool:
                        tm = 'bool'
                    elif df[cname].dtype == np.dtype('datetime64[ns]'):
                        tm = 'datetime'
                    elif df[cname].dtype == np.dtype('timedelta64[ns]'):
                        tm = 'timedelta'
                    elif type(df[cname][0]) == date:
                        tm = 'date'
                    elif type(df[cname][0]) == time:
                        tm = 'time'
                    elif type(df[cname][0]) == Point and df[cname][0].has_z:
                        tm = 'point3'
                    elif type(df[cname][0]) == Point:
                        tm = 'point'
                    else:
                        tm = 'text'

                    self._put('nodes', nid, 'types', tm)
                    #cmap[cname] = nid
                    #dmap[cname] = []
                    #tmap[cname] = tm
                    return (cname, nid, tm)
                except Exception as e:
                    retries -= 1
                    sleep(0.5)
            raise Exception("failed to create node/index")


        with ThreadPoolExecutor(max_workers=1) as pool:
            res = pool.map(add, [x for x in df.columns if x not in ['start', 'end']])
            for cname, nid, tm in res:
                cmap[cname] = nid
                dmap[cname] = []
                tmap[cname] = tm


        origin = self.origin
        res = []
        q = Queue()
        wt = Thread(target=worker, args=(q, workers, len(df) * (len(df.columns) - 1), self._parent.interactive))
        wt.start()

        for i, row in df.iterrows():
            if origin is not None:
                ts = int((dt64(row['start']) - origin) // np.timedelta64(1, 'ns'))
            else:
                ts = int(td64(row['start']) // np.timedelta64(1, 'ns'))
            try:
                if 'end' in row and origin is not None:
                    dur = int((dt64(row['end']) - dt64(row['start'])) // np.timedelta64(1, 'ns'))
                elif origin is not None:
                    dur = int((dt64(df['start'].iloc[i+1]) - dt64(row['start'])) // np.timedelta64(1, 'ns'))
                elif 'end' in row:
                    dur = int((td64(row['end']) - td64(row['start'])) // np.timedelta64(1, 'ns'))
                else:
                    dur = int((td64(df['start'].iloc[i+1]) - td64(row['start'])) // np.timedelta64(1, 'ns'))
            except IndexError:
                dur = 1

            if dur <= 0: continue
            for col, val in dict(row).items():
                if col == 'start' and 'start' in dmap:
                    dmap[col].append((ts, dur))
                elif type(val) == float and math.isnan(val): # skip nans
                    pass
                elif val is pd.NaT or val is None:
                    pass
                elif col not in tmap:
                    pass
                elif tmap[col] == 'point3':
                    dmap[col].append((ts, dur, (val.x, val.y, val.z)))
                elif tmap[col] == 'point':
                    dmap[col].append((ts, dur, (val.x, val.y)))
                elif tmap[col] == 'date':
                    dmap[col].append((ts, dur, val.isoformat()))
                elif tmap[col] == 'time':
                    dmap[col].append((ts, dur, val.isoformat()))
                elif tmap[col] == 'datetime':
                    dmap[col].append((ts, dur, iso8601(val)))
                elif tmap[col] == 'timedelta':
                    dmap[col].append((ts, dur, val // np.timedelta64(1, 'ns')))
                else:
                    dmap[col].append((ts, dur, val))

            if 'start' in dmap and len(dmap['start']) >= chunksize:
                q.put([(self, cmap[k], tmap[k], dmap[k]) for k, v in dmap.items()])
                for k, v in dmap.items():
                    dmap[k] = []
            elif len(list(dmap.values())[0]) >= chunksize:
                q.put([(self, cmap[k], tmap[k], dmap[k]) for k, v in dmap.items()])
                for k, v in dmap.items():
                    dmap[k] = []

        if 'start' in dmap and len(dmap['start']) > 0:
            q.put([(self, cmap[k], tmap[k], dmap[k]) for k, v in dmap.items()])
            for k, v in dmap.items():
                dmap[k] = []
        elif len(list(dmap.values())[0]) > 0:
            q.put([(self, cmap[k], tmap[k], dmap[k]) for k, v in dmap.items()])
            for k, v in dmap.items():
                dmap[k] = []
        q.put([])
        wt.join()
        
            


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

class Node(API):
    def __init__(self, db, node):
        self._node = node
        API.__init__(self, db._credentials, *db._prefix, "nodes", self._node)

    @property
    def meta(self):
        return Metadata(self)

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

    def __setitem__(self, key, v):
        if not isinstance(key, tuple):
            key = (key,)

        self._parent[self._path + key] = v

    @property
    def links(self):
        return iter(self)

    @property
    def children(self):
        return iter((self[k] for k in self.links))


    def __iter__(self):
        r = self._get("links")
        if r.status_code != 200:
            raise SentenaiError("invalid response")
        data = r.json()
        return iter(sorted(data.keys()))

    def insert(self, values):
        vs = []
        origin = self._parent.origin
        if origin is not None:
            for v in values:
                start = (dt64(v['start']) - origin) // np.timedelta64(1, 'ns')
                end = (dt64(v['end']) - origin) // np.timedelta64(1, 'ns')
                if 'value' in v:
                    vs.append((int(start), int(end - start), v['value']))
                else:
                    vs.append((int(start), int(end - start)))
        else:
            for v in values:
                start = td64(v['start']) // np.timedelta64(1, 'ns')
                end = td64(v['end']) // np.timedelta64(1, 'ns')
                if 'value' in v:
                    vs.append((int(start), int(end - start), v['value']))
                else:
                    vs.append((int(start), int(end - start)))
        self._post('types', self.type,
                json=cbor2.dumps(vs), headers={'Content-Type': 'application/cbor'}, raw=True)

    def export(self, start=None, end=None, limit=None, exclude=tuple(), origin=datetime(1970,1,1), when=None):
        exp = API(self._credentials, "export")
        o = iso8601(self._parent.origin or origin)[:-1] + 'Z'
        co = ["start", "end"] 
        cols = [f"{self}/{x}" for x in iter(self) if x not in exclude]
        when = when or str(self)
        params = {}
        if limit is not None:
            params['limit'] = limit
        if start is not None:
            params['start'] = iso8601(start) if o else int(start)
        if end is not None:
            params['end'] = iso8601(end) if o else int(end) 
        if o is not None:
            params['origin'] = o

        r = exp._post(json={'when': when, 'select': co+cols}, params=params)
        return pd.DataFrame(
                r.json(),
                columns = co + [x for x in list(self) if x not in exclude],
                ).astype({
                    'start': np.dtype('datetime64[ns]'),
                    'end': np.dtype('datetime64[ns]')
                })


    def graph(self, limit=-1):
        return self._parent.graph(self._path, limit)

    @property
    def source(self):
        r = self._get()
        if r.status_code == 200:
            info = r.json()
            if not info:
                return None
            return info.get('tspl')
        else:
            return None



    @property
    def meta(self):
        return Metadata(self)

    @meta.setter
    def meta(self, md):
        payload = {}
        for key, val in md.items():
            if isinstance(val, bool):
                vtype = "bool"
            elif isinstance(val, datetime) or isinstance(val, np.datetime64):
                vtype = "datetime"
                val = dt64(val)
            elif isinstance(val, int):
                vtype = "int"
            elif isinstance(val, float):
                vtype = "float"
            else:
                vtype = "text"
            payload[key] = {'type': vtype, 'value': val}
        resp = self._patch('meta', json=payload)
        if resp.status_code not in [200, 201, 204]:
            raise Exception(resp.status_code)


    @property
    def type(self):
        r = self._get('types')
        if r.status_code == 200:
            ts = r.json()
            if not ts:
                return None
            else:
                return ts[0]
        else:
            return None

    @property
    def range(self):
        if self.type is None:
            return None
        r = self._get('types', self.type, 'range')
        if r.status_code == 200:
            e = r.json()
            if e is None:
                return None
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
    def raw(self):
        return RawData(self)


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

    #def __len__(self):
    #    return StreamStats(self, vtype=self.type, ro=True).count
    
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
                {stat}({"when " if self._type == 'event' else ''}{self._parent})
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
    def __init__(self, parent, index, df=False, resample=None, rolling=None, origin=''):
        self._parent = parent
        self._df = df
        self._type = index
        self._origin = origin
        self._resample = resample
        self._rolling = rolling
        API.__init__(self, parent._credentials, *parent._prefix, "types", index)

    def origin(self, o=None):
        if not o:
            return StreamData(self._parent, self._type, self._df, self._resample, self._rolling, '')
        else:
            return StreamData(self._parent, self._type, self._df, self._resample, self._rolling, 'origin ' + iso8601(o))

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
            return self._parent._parent._parent.df(rs + self._origin)[tr]
        else:
            return self._parent._parent._parent(rs + self._origin)[tr]



class RawData(API):
    def __init__(self, parent):
        self._parent = parent

    def __getitem__(self, i):
        params = {}
        if isinstance(i, slice):

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
        if self._parent.type == 'event':
            resp = self._parent._parent._parent._post('tspl', json=f'when {self._parent!s}', params=params, headers={'Accept': 'application/cbor'})
        else:
            resp = self._parent._parent._parent._post('tspl', json=str(self._parent), params=params, headers={'Accept': 'application/cbor'})
        if 'content-type' in resp.headers and resp.headers['content-type'] == 'application/cbor':
            return cbor2.loads(resp.content)
        else:
            return resp.json()



