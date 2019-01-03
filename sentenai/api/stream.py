from __future__ import print_function
import json as JSON
import pytz
from copy import copy
import re, sys, time, base64, random
import requests

import numpy as np
import pandas as pd

from pandas.io.json import json_normalize
from datetime import timedelta
from functools import partial
from multiprocessing.pool import ThreadPool
from threading import Lock
from shapely.geometry import Point

from sentenai.exceptions import *
from sentenai.exceptions import handle
from sentenai.utils import *
from sentenai.historiQL import EventPath, Stream, StreamPath, Proj, check_proj, ProjAgg

BaseStream = Stream
V = EventPath()

if PY3:
    string_types = str
else:
    string_types = basestring

if not PY3:
    import virtualtime

try:
    from urllib.parse import quote
except:
    from urllib import quote

class Stream(object):
    def __init__(self, client, id, filters=None):
        self._client = client
        self.id = self.name = self._name = quote(id.encode('utf-8'))
        self.filters = filters
        self.meta = StreamMetadata(self)

    def __repr__(self):
        return f'Stream(id="{self.id}")'

    def get(self, *parts, params={}, headers={}):
        if self.filters: params['filters'] = self.filters()
        return self._client.get(*(['streams', self.id] + list(parts)), params=params, headers=headers)

    def put(self, *parts, params={}, headers={}, json={}):
        if self.filters: params['filters'] = self.filters()
        return self._client.put(*(['streams', self.id] + list(parts)), params=params, headers=headers, json=json)

    def post(self, *parts, params={}, headers={}, json={}):
        if self.filters: params['filters'] = self.filters()
        return self._client.post(*(['streams', self.id] + list(parts)), params=params, headers=headers, json=json)

    def patch(self, *parts, params={}, headers={}, json={}):
        if self.filters: params['filters'] = self.filters()
        return self._client.patch(*(['streams', self.id] + list(parts)), params=params, headers=headers, json=json)

    def delete(self, *parts, params={}, headers={}):
        if self.filters: params['filters'] = self.filters()
        return self._client.delete(*(['streams', self.id] + list(parts)), params=params, headers=headers)

    def __len__(self):
        resp = self.get(params={'stats': True})
        if resp.status_code == 404:
            return 0
        elif resp.status_code == 200:
            data = resp.json()
            return int(data.get('events', 0))
        else:
            raise SentenaiException(resp.status_code)

    def __bool__(self):
        resp = self.get()
        if resp.status_code == 404:
            return False
        elif resp.status_code == 200:
            return True
        else:
            handle(resp)

    __nonzero__ = __bool__

    def __enter__(self):
        return BaseStream(self._name, None, self.filters)

    def __exit__(self, *args, **kwargs):
        pass

    def log(self, event=None, ts=None, id=None, duration=None):
        data = {} if event is None else event
        self._client._queue.put(self.Event(data=data, ts=ts, id=id, duration=duration))


    def upload(self, file_path, id=None, ts="timestamp", duration=None, threads=4, apply=dict):
        def f(row):
            timestamp = row[ts]
            dur = row[duration] if duration else None
            eid = row[id] if uid else None
            d = apply(row)
            if apply is dict:
                del d[ts]
                if eid is not None:
                    del d[id]
                if dur is not None:
                    del d[duration]
            self.Event(id=eid, ts=timestamp, duration=dur, data=d).create()
            time.sleep(.01)

        with open(file_path) as fobj:
            fobj.seek(0)
            num_lines = sum(1 for line in fobj) - 1
            errors = 0
        with ThreadPool(threads) as pool:
            if self._client.notebook:
                for x in log_progress(pd.read_csv(file_path, chunksize=threads), size=num_lines):
                    pool.map(f, [r for i, r in x.iterrows()])
            else:
                for x in pd.read_csv(file_path, chunksize=threads):
                    pool.map(f, [r for i, r in x.iterrows()])

    def where(self, filters):
        """Return stream with additional filters applied.
        """
        return Stream(self, self._client, self.id, None if filters is None else self.filters & filters)

    filtered = where

    @property
    def oldest(self):
        """Get the oldest event by timestamp in this stream.
        """
        resp = self.get("oldest", params={'filters': self.filters})
        if resp.status_code == 200:
            return Event(self._client, self, resp.headers['Location'], cts(resp.headers['Timestamp']), resp.json(), saved=True)
        elif resp.status_code == 404:
            return None
        else:
            raise SentenaiException(resp.status_code)

    @property
    def newest(self):
        """Get the newest event by timestamp in this stream.
        """
        resp = self.get("newest", params={'filters': self.filters})
        if resp.status_code == 200:
            return Event(self._client, self, resp.headers['Location'], cts(resp.headers['Timestamp']), resp.json(), saved=True)
        elif resp.status_code == 404:
            return None
        else:
            raise SentenaiException(resp.status_code)

    earliest = oldest
    latest = newest

    @property
    def fields(self):
        """Get a view of all fields in this stream."""
        resp = self.get('fields')
        if resp.status_code == 200:
            return Fields(self, resp.json())
        elif resp.status_code == 404:
            return None
        else:
            raise SentenaiException(resp.status_code)

    def _fields(self, start=None, end=None):
        """Get a view of all fields in this stream."""
        resp = self.get('fields')
        if resp.status_code == 200:
            return Fields(self, resp.json(), start=start, end=end)
        elif resp.status_code == 404:
            return None
        else:
            raise SentenaiException(resp.status_code)

    def tags(self):
        """Get a view of all tags in this stream."""
        return Fields(self._client, self, view="tag")

    def stats(self, field, start=None, end=None):
        """Get stats for a given numeric field.

           Arguments:
           field  -- A dotted field name for a numeric field in the stream.
           start  -- Optional argument indicating start time in stream for calculations.
           end    -- Optional argument indicating end time in stream for calculations.
        """
        args = {}
        if start:
            args['start'] = start.isoformat() + ("Z" if not start.tzinfo else "")
        if end:
            args['end'] = end.isoformat() + ("Z" if not end.tzinfo else "")

        return self.get(str(field), 'stats', params=args)

    def values(self, at=None):
        """Get current values for every field in a stream.

        Keyword Arguments:
            at -- If given a datetime for `at`, return the values at that point in
                  time instead
        """
        resp = self.get("values", params={'at': iso8601(at or datetime.utcnow())})
        if resp.status_code == 404:
            return None
        elif resp.status_code == 200:
            return Values(self, at, resp.json())

    def Event(self, *args, **kwargs):
        return Event(self._client, self, *args, **kwargs)

    def describe(self, field, start=None, end=None):
        """Describe a given numeric field.

           Arguments:
           field  -- A dotted field name for a numeric field in the stream.
           start  -- Optional argument indicating start time in stream for calculations.
           end    -- Optional argument indicating end time in stream for calculations.
        """
        x = self._client.field_stats(self, field, start, end)
        if x.get('categorical'):
            print("count\t{count}\nunique\t{unique}\ntop\t{top}\nfreq\t{freq}".format(**x['categorical']))
        else:
            p = x['numerical']
            print("count\t{}\nmean\t{:.2f}\nstd\t{:.2f}\nmin\t{}\nmax\t{}".format(
                p['count'], p['mean'], p['std'], p['min'], p['max']))

    def __getitem__(self, s):
        if type(s) == int:
            if s == 0:
                return self.oldest
            elif s == -1:
                return self.newest
            else:
                raise NotImplemented

        elif type(s) == slice:
            if s.start is None and s.stop is None and s.step is not None:
                if s.step < 0:
                    return self[self.newest.ts:self.oldest.ts - timedelta(microseconds=1):s.step]
                else:
                    return self[self.oldest.ts:self.newest.ts + timedelta(microseconds=1):s.step]
            # replace None start/stop with oldest/newest
            if s.start is None:
                start = self.oldest.ts
            if s.stop is None:
                stop = self.newest.ts + timedelta(microseconds=1)
            # select start ts by id, datetime or timedelta
            if type(s.start) is str:
                start = self[s.start].ts
            elif isinstance(s.start, datetime):
                start = s.start
            elif isinstance(s.start, timedelta):
                start = stop - s.start
            # select end ts by id, datetime or timedelta
            if type(s.stop) is str:
                stop = self[s.stop].ts
            elif isinstance(s.stop, datetime):
                stop = s.stop
            elif isinstance(s.stop, timedelta):
                stop = start + s.stop
            # add limit via the step argument
            if s.step and s.step < 0:
                return self.range(stop, start, abs(s.step), sorting='desc')
            elif s.step:
                return self.range(start, stop, s.step, sorting='asc')
            else:
                return self.range(start, stop, sorting='asc')
        elif type(s) == str:
            x = self.Event(id=s).read()
            if x is None:
                raise KeyError
            else:
                return x
        elif type(s) == datetime:
            return self.values(at=s)
        else:
            raise ValueError("wrong type")

    def __setitem__(self, s, data):
        if type(s) == str:
            self.Event(id=s, data=data).create()

        elif isinstance(s, datetime):
            self.Event(ts=s, data=data).create()
        elif isinstance(s, slice):
            self.Event(ts=s.start, duration=s.stop-s.start, data=data).create()

        elif type(s) == tuple and 1 < len(s) < 4:
            if len(s) == 2:
                if type(s[0]) == str:
                    id, ts = s
                    if type(ts) == slice:
                        dur = ts.stop - ts.start
                        ts = ts.start
                        self.Event(id=id, ts=ts, duration=dur, data=data).create()
                else:
                    ts, dur = s
                    self.Event(ts=ts, duration=dur, data=data).create()
            else:
                id, ts, dur = s
                self.Event(id=id, ts=ts, duration=dur, data=data).create()
        else:
            raise ValueError("Invalid event definition")

    def __delitem__(self, s):
        print(s)
        if type(s) == datetime:
            raise ValueError("wrong type")
        elif type(s) == slice:
            self.get('start', s.start, 'end', s.stop)
            for x in self.__getitem__(s):
                x.delete()
        else:
            self[s].delete()


    def range(self, start, end, limit=None, sorting='asc'):
        """Get all of a stream's events between start (inclusive) and end (exclusive).

        Arguments:
           start  -- A datetime object representing the start of the requested
                     time range.
           end    -- A datetime object representing the end of the requested
                     time range.

           Result:
           A time ordered list of all events in a stream from `start` to `end`
        """
        params = {'limit': limit, 'sort': sorting}
        resp = self.get('start', iso8601(start), 'end', iso8601(end), params=params)
        if resp.status_code == 200:
            evts = [self.Event(saved=True, **data) for data in json.loads(resp.text)]
            if limit is None:
                return Events(self, evts, start=start, end=end)
            elif evts:
                if sorting == 'asc':
                    return Events(self, evts, start=evts[0].ts, end=evts[-1].ts)
                else:
                    return Events(self, evts, start=evts[-1].ts, end=evts[0].ts)
            else:
                return Events(self, [], start, end)
        else:
            raise SentenaiException(resp.status_code)

    def tail(self, n=5):
        """Get all of a stream's events between start (inclusive) and end (exclusive).

        Arguments:
           n      -- A max number of events to return

           Result:
           A time ordered list of all events in a stream from `start` to `end`
        """
        return self[::-1 * n]

    def head(self, n=5):
        """Get all of a stream's events between start (inclusive) and end (exclusive).

        Arguments:
           n      -- A max number of events to return

           Result:
           A time ordered list of all events in a stream from `start` to `end`
        """
        return self[::n]




class Event(object):
    def __init__(self, client, stream, id=None, ts=None, data=None, event=None, duration=None, saved=False):
        self.stream = stream
        self.id = id
        self.ts = ts if isinstance(ts, datetime) or ts is None else cts(ts)
        self.data = data or event or {}
        self.duration = duration
        self._saved = saved

    """
    def get(self, *parts, params={}, headers={}):
        if self.filters: params['filters'] = self.filters()
        return self.stream.get(*(['events', self.id] + list(parts)), params=params, headers=headers)

    def put(self, *parts, params={}, headers={}):
        if self.filters: params['filters'] = self.filters()
        return self.stream.put(*(['events', self.id] + list(parts)), params=params, headers=headers)
    """

    @property
    def exists(self):
        return bool(self._saved)

    def __repr__(self):
        return "Event(stream={}, id={}, ts={}, exists={})".format(self.stream.id, self.id, self.ts, self.exists)

    def _repr_html_(self):
        return '<pre>Event(\n  stream = "{}",\n  id = "{}",\n  ts = {},\n  exists = {},\n  data = {})</pre>'.format(self.stream.id, self.id, repr(self.ts), self.exists, JSON.dumps(self.data, indent=4, default=dts))


    def json(self, include_id=False, df=False):
        if df:
            d = copy(self.data)
            d['ts'] = pd.to_datetime(dts(self.ts))
            return d
        elif include_id:
            return {'ts': self.ts, 'event': self.data, 'id': self.id}
        else:
            return {'ts': self.ts, 'event': self.data}

    def create(self):
        if self.ts and self.duration:
            headers = {'start': iso8601(self.ts), 'end': iso8601(self.ts + self.duration)}
        elif self.ts:
            headers = {'ts': iso8601(self.ts)}
        elif self.ts is None and self.duration is None:
            headers = {}
        else:
            raise ValueError("Can't specify duration without timestamp.")
        if self.id:
            resp = self.stream.put('events', self.id, headers=headers, json=self.data)
        else:
            resp = self.stream.post('events', headers=headers, json=self.data)
        if resp.status_code in [200, 201]:
            loc = resp.headers['Location']
            self.id = loc
            self._saved = True
            return self
        elif resp.status_code >= 500:
            raise SentenaiException('retry later')
        elif resp.status_code == 404:
            raise NotFound
        else:
            raise Exception(resp.status_code)

    def read(self):
        resp = self.stream.get("events", self.id)
        if resp.status_code == 200:
            self.ts = cts(resp.headers['Timestamp'])
            self.duration = cts(resp.headers['Duration']) if 'Duration' in resp.headers else None
            self.data = resp.json()
            self._saved = True
            return self
        elif resp.status_code == 404:
            return None
        else:
            raise SentenaiException(resp.status_code)

    def update(self):
        if self.ts and self.duration:
            headers = {'start': iso8601(self.ts), 'end': iso8601(self.ts + self.duration)}
        elif self.ts:
            headers = {'ts': iso8601(self.ts)}
        elif self.ts is None and self.duration is None:
            headers = {}
        else:
            raise ValueError("Can't specify duration without timestamp.")
        if self.id:
            resp = self.stream.put('events', self.id, headers=headers, json=self.data)
        else:
            raise KeyError("Event does not exist yet. Use create instead.")
        self._saved = True

    def delete(self):
        resp = self.stream.delete('events', self.id)
        if resp.status_code in [200, 204]:
            self._saved = False
        else:
            raise NotFound


class Values(object):
    def __init__(self, stream, at, values):
        values_rendered = []
        for value in values:
            # create path
            ts = cts(value['ts'])
            values_rendered.append({
                'timestamp': ts,
                'event': value['id'],
                'value': value['value'],
                'path': value['path'],
            })
        self.at = at
        self._data = values_rendered

    def __getitem__(self, i):
        if type(i) == str:
            i = (i,)
        if type(i) == tuple:
            for d in self._data:
                if list(i) == d['path']:
                    return d['value']
            else:
                raise IndexError
        else:
            return self._data[i]

    def _repr_html_(self):
        df = pd.DataFrame(self._data)
        if df.empty:
            return df._repr_html_()
        df['path'] = df['path'].apply(lambda x: ".".join(x))
        df = df[['path', 'value', 'timestamp']]
        return df.sort_values(by='path').rename(
                index=str,
                columns={
                    'path': 'Event Path',
                    'value': 'Value'.format(self.at),
                    'timestamp': 'Updated At'
                    }
            )._repr_html_()

    def items(self):
        return [(d['path'], d['value']) for d in self._data]


class Field(object):
    def __init__(self, stream, path, start=None, end=None):
        self._stream = stream
        self._path = path
        self._stats = None
        self._start = start
        self._end = end

    def __repr__(self):
        return ".".join(self._path)

    @property
    def mean(self):
        try:
            return self.stats['numerical']['mean']
        except KeyError:
            raise TypeError

    @property
    def min(self):
        try:
            return self.stats['numerical']['min']
        except KeyError:
            raise TypeError

    @property
    def max(self):
        try:
            return self.stats['numerical']['max']
        except KeyError:
            raise TypeError

    @property
    def std(self):
        try:
            return self.stats['numerical']['std']
        except KeyError:
            raise TypeError

    @property
    def missing(self):
        try:
            if self.stats['numerical']:
                return self.stats['numerical']['missing']
            elif self.stats['categorical']:
                return self.stats['categorical']['missing']
            else:
                raise TypeError
        except KeyError:
            raise TypeError

    @property
    def count(self):
        try:
            if self.stats['numerical']:
                return self.stats['numerical']['count']
            elif self.stats['categorical']:
                return self.stats['categorical']['count']
            else:
                raise TypeError
        except KeyError:
            raise TypeError

    @property
    def unique(self):
        try:
            return self.stats['categorical']['unique']
        except KeyError:
            raise TypeError

    @property
    def top(self):
        try:
            return self.stats['categorical']['top']
        except KeyError:
            raise TypeError

    @property
    def freq(self):
        try:
            return self.stats['categorical']['freq']
        except KeyError:
            raise TypeError

    @property
    def stats(self):
        if self._stats is None:
            params = {'start': self._start, 'end': self._end}
            resp = self._stream.get('fields', ".".join(["event"] + self._path), 'stats', params=params)
            if resp.status_code == 200:
                data = resp.json()
                self._stats = data
                return data
            else:
                raise SentenaiException("can't get stats")
        else:
            return self._stats

    @property
    def values(self):
        if self.stats['categorical']:
            params = {'start': self._start, 'end': self._end}
            resp = self._stream.get('fields', ".".join(["event"] + self._path), 'values', params=params)
            if resp.status_code == 200:
                data = resp.json()
                return Unique(data)
            else:
                raise TypeError
        else:
            raise TypeError


class Unique(object):
    def __init__(self, u):
        if u['categorical']:
            self.unique = u['categorical']
        elif u['numerical']:
            self.unique = u['numerical']
        else:
            self.unique = []

    def _repr_html_(self):
        if self.unique:
            return pd.DataFrame([{"value": k, "frequency": v} for k, v in self.unique])[["value","frequency"]]._repr_html_()
        else:
            return pd.DataFrame()._repr_html_()

    def __getitem__(self, x):
        if type(x) == str:
            for k, v  in self.unique:
                if k == x:
                    return v
            else:
                raise KeyError
        elif type(x) == int or type(x) == slice:
            return self.unique[x][0]
        else:
            raise ValueError






class Fields(object):
    def __init__(self, stream, fields, view="field", start=None, end=None):
        self.stream = stream
        self._view = view
        self._fields = fields
        self._start = start
        self._end = end

    def __getitem__(self, s):
        if type(s) is slice:
            return Fields(self.stream, self._fields[s], self._view, start=self._start, end=self._end)
        elif type(s) is int:
            return Field(self.stream, self._fields[s]['path'], start=self._start, end=self._end)
        elif type(s) is str:
            for field in self._fields:
                if len(field['path']) == 1 and field['path'][0] == s:
                    return Field(self.stream, field['path'], start=self._start, end=self._end)
            else:
                raise KeyError
        elif type(s) is tuple and all([type(x) == str for x in s]):
            for field in self._fields:
                if field['path'] == list(s):
                    return Field(self.stream, field['path'], start=self._start, end=self._end)
            else:
                raise KeyError

    def __repr__(self):
        return repr(self._fields)

    def __iter__(self):
        return (Field(self.stream, x['path'], start=self._start, end=self._end) for x in self._fields)

    def _repr_html_(self):
        df = pd.DataFrame(sorted(self._fields, key=lambda x: (x['start'], ".".join(x['path']))))
        df['path'] = df['path'].apply(lambda x: ".".join(x))
        df['start'] = df['start'].apply(cts)
        df = df[['path', 'start', 'id']]
        return df.rename(
                index=str,
                columns={
                    'path': self._view.capitalize(),
                    'start': 'Added At',
                    'id': 'Added by (event id)'
                    }
            )._repr_html_()



class StreamMetadata(object):
    def __init__(self, stream):
        self._stream = stream
        self._meta = {}


    def read(self):
        resp = self._stream.get('meta')
        if resp.status_code == 404:
            return None
        elif resp.status_code == 200:
            data = resp.json()
            parsed = {}
            for k,v in data.items():
                if type(v) in [float, int, bool]:
                    parsed[k] = v
                elif type(v) == dict and 'lat' in v and 'lon' in v:
                    parsed[k] = Point(v['lon'], v['lat'])
                else:
                    for fmt in ["%Y-%m-%dT%H:%M:%S.%fZ","%Y-%m-%dT%H:%M:%SZ","%Y-%m-%dT%H:%M:%S","%Y-%m-%dT%H:%M:%S.%f"]:
                        try:
                            val = datetime.strptime(v, fmt)
                        except ValueError:
                            pass
                        else:
                            parsed[k] = val
                            break
                    else:
                        parsed[k] = v

            return parsed
        else:
            return SentenaiException()

    def update(self, kvs):
        kvs2 = {}
        for k, v in kvs.items():
            if v is None:
                kvs2[k] = None
            else:
                kvs2[k] = dts(v)
        if self._stream:
            self._stream.patch("meta", json=kvs2)
        else:
            self._stream.post("meta", json=kvs2)

    def replace(self, kvs):
        kvs2 = {}
        for k, v in kvs.items():
            kvs2[k] = dts(v)
        if self._stream:
            self._stream.put("meta", json=kvs2)
        else:
            self._stream.post("meta", json=kvs2)

    def clear(self):
        if self._stream:
            self._stream.put("meta")

    def __repr__(self):
        self._meta = self.read()
        return repr(self._meta)

    def _type(self, v):
        if type(v) in [int, float]:
            return "Numeric"
        elif type(v) == datetime:
            return "Datetime"
        elif type(v) == bool:
            return "Boolean"
        else:
            return "String"

    def _repr_html_(self):
        xs = []
        self._meta = self.read()
        if self._meta:
            for f,v in self._meta.items():
                xs.append({'field': f, 'value': str(v), 'type': self._type(v)})

        return pd.DataFrame(xs, columns=["field", "value", "type"])._repr_html_()

    def __getitem__(self, key):
        self._meta = self.read()
        return self._meta()[key]

    def __setitem__(self, key, val):
        self._meta = self.read()
        self.update({key: val})

    def __delitem__(self, key):
        x = self._meta = self.read()
        try:
            del x[key]
        except:
            print("not there")
        else:
            self.replace(x)


class Events(object):
    def __init__(self, stream, events, start=None, end=None):
        self.events = events
        self.stream = stream
        self.start = start
        self.end = end

    def __iter__(self):
        return iter(self.events)

    def __getitem__(self, s):
        if type(s) == slice:
            return Events(self.stream, self.events[s], start=self.start, end=self.end)
        else:
            return self.events[s]

    def _repr_html_(self):
        if self.events:
            return pd.DataFrame([{'id': x.id, 'ts': x.ts, 'duration': x.duration} for x in self.events])[['id', 'ts', 'duration']]._repr_html_()
        else:
            return pd.DataFrame()._repr_html_()

    def __len__(self):
        return len(self.events)

    @property
    def fields(self):
        return self.stream._fields(self.start, self.end)




class Result(object):
    def __init__(self, streams, start, end, limit=None, sorting='asc', fill=None, freq=None):
        self.streams = streams
        self._events = []
        self.sort = sorting
        self.limit = limit
        self.start = start
        self.end = end
        self.frequency = freq
        self.fill = fill

    def resample(self, freq):
        self.frequency = freq
        return self


    def events(self):
        data = {'select': {'expr': 'true'}}
        self.stream




class StreamRange(object):
    def __init__(self, stream, start, end, limit=None, sorting="asc"):
        self.stream = stream
        self._events = []
        self.sort = sorting
        self.limit = limit
        self.start = start
        self.end = end
        self.frequency = None
        self.fill = None

    def __matmul__(self, d):
        if type(d) is tuple:
            shift = d[1] if len(d) == 3 else None
            if len(d) == 3:
                window, shift, features = d
            elif len(d) == 2:
                window, features = d
            if type(window) is tuple:
                lag, horiz = window
            else:
                lag, horiz = window, 0
            return self.reshape(lag=lag, horizon=horiz, features=(features,) if isinstance(features, ProjAgg) else features)
        elif self.frequency is None:
            if check_proj(d) == "agg":
                return self.agg(**d)
            else:
                return self.df(**d)
        else:
            return self.agg(**d)

    def __getitem__(self, i):
        params = {'limit': self.limit, 'sort': self.sort}
        resp = self.stream.get('start', iso8601(self.start), 'end', iso8601(self.end), params=params)
        if resp.status_code == 200:
            self._events = [self.stream.Event(saved=True, **data) for data in json.loads(resp.text)]
            return self._events[i]
        else:
            raise SentenaiException(resp.status_code)

    def __iter__(self):
        params = {'limit': self.limit, 'sort': self.sort}
        resp = self.stream.get('start', iso8601(self.start), 'end', iso8601(self.end), params=params)
        if resp.status_code == 200:
            self._events = [self.stream.Event(saved=True, **data) for data in json.loads(resp.text)]
            return iter(self._events)
        else:
            raise SentenaiException(resp.status_code)

    def resample(self, freq):
        self.frequency = freq
        return self

    def agg(self, *args, **kwargs):
        for arg in args:
            # field renames
            if isinstance(arg, dict):
                for k, v in arg.items():
                    kwargs[k] = v
                continue

            base = kwargs
            segments = list(arg)
            for a in segments[:-1]:
                x = kwargs.get(a)
                if isinstance(x, dict):
                    base = x
                else:
                    base = base[a] = {}
            else:
                base[segments[-1]] = arg

        if check_proj(kwargs) != "agg":
            raise Exception("Must use aggregation projections with `agg`")

        if self.frequency is None:
            self.frequency = "9000AS"



        params = {}
        if kwargs:
            with self.stream as s:
                p = Proj(s, kwargs, resample=self.frequency)()['projection']
                params['projection'] = p
        params['limit'] = self.limit
        params['sort'] = self.sort
        params['frequency'] = self.frequency
        params['fill'] = self.fill
        resp = self.stream.get('start', iso8601(self.start), 'end', iso8601(self.end), params=params)

        if resp.status_code == 200:
            self._events = [self.stream.Event(**data) for data in json.loads(resp.text)]
        else:
            raise SentenaiException(resp.status_code)

        if len(self._events):
            if self.sort == "desc":
                self._events = reversed(self._events)
            f = json_normalize([x.json(df=True) for x in self._events])
            return f.set_index('ts')
        else:
            return pd.DataFrame()

    def df(self, *args, **kwargs):
        if self.frequency is not None:
            raise Exception("Cannot call `.df()` on resampled data.")
        for arg in args:
            # field renames
            if isinstance(arg, dict):
                for k, v in arg.items():
                    kwargs[k] = v
                continue

            base = kwargs
            segments = list(arg)
            for a in segments[:-1]:
                x = kwargs.get(a)
                if isinstance(x, dict):
                    base = x
                else:
                    base = base[a] = {}
            else:
                base[segments[-1]] = arg

        with self.stream as s:
            p = Proj(s, kwargs)()['projection']


        params = {}
        if kwargs:
            with self.stream as s:
                p = Proj(s, kwargs, resample=self.frequency)()['projection']
                params['projection'] = p
        params['limit'] = self.limit
        params['sort'] = self.sort
        params['frequency'] = self.frequency
        params['fill'] = self.fill
        resp = self.stream.get('start', iso8601(self.start), 'end', iso8601(self.end), params=params)
        if resp.status_code == 200:
            self._events = [self.stream.Event(**data) for data in json.loads(resp.text)]
        else:
            raise SentenaiException(resp.status_code)

        if len(self._events):
            if self.sort == "desc":
                self._events = reversed(self._events)
            f = json_normalize([x.json(df=True) for x in self._events])
            return f.set_index('ts')
        else:
            return pd.DataFrame()


    def reshape(self, *args, lag, horizon, features):
        if args and not features:
            features = args
        kwargs = dict(("feature-{:04d}".format(i), arg) for i, arg in enumerate(features))
        keys = list(sorted(kwargs.keys()))
        params = {}
        if kwargs:
            with self.stream as s:
                p = Proj(s, kwargs, resample=self.frequency)()['projection']
                params['projection'] = p
        params['limit'] = self.limit
        params['sort'] = self.sort
        params['frequency'] = self.frequency
        params['fill'] = self.fill
        resp = self.stream.get('start', iso8601(self.start), 'end', iso8601(self.end), params=params)
        if resp.status_code == 200:
            self._events = [self.stream.Event(**data) for data in json.loads(resp.text)]
        else:
            raise SentenaiException(resp.status_code)

        def tensors():
            r = self._events
            for i in range(len(r) - lag - horizon):
                yield ( np.array([[[e.data.get(k) for k in keys] for e in r[i:i+lag]]], np.float32)
                      , np.array([[e.data.get(k) for k in keys] for e in r[i+lag:i+lag+horizon]], np.float32)
                      )

        return TD(tensors())

    def _repr_html_(self):
        return self.df()._repr_html_()


class TD(object):
    def __init__(self, xy):
        self.xy = xy
        self.validation_split = None
        self.vbuffer = []
        self.tbuffer = []

    def _get_next(self, test=True):
        if test and self.tbuffer:
            return self.tbuffer.pop(0)
        elif test:
            while True:
                if self.validation_split is None or random.random() >= self.validation_split:
                    return next(self.xy)
                else:
                    self.vbuffer.append(next(self.xy))
        elif self.vbuffer:
            return self.vbuffer.pop(0)
        else:
            while True:
                if self.validation_split and random.random() < self.validation_split:
                    return next(self.xy)
                else:
                    self.tbuffer.append(next(self.xy))


    def validation(self, split=0.2):
        self.validation_split = split
        while True:
            yield self._get_next(test=False)

    def test(self):
        while True:
            p = self._get_next()
            yield p


class StreamsView(object):
    def __init__(self, client, streams):
        self._client = client
        self._streams = streams

    def _repr_html_(self):
        if self._streams:
            return pd.DataFrame(self._streams)[['name', 'events', 'healthy']].rename(columns={'events': 'length'})._repr_html_()
        else:
            return pd.DataFrame(columns=["name", "length", "healthy"])._repr_html_()

    def __iter__(self):
        return iter([Stream(
            self._client,
            name=v['name'],
            tz=v.get('tz', None),
            exists=True
         ) for v in self._streams])

    def __getitem__(self, i):
        v = self._streams[i]
        return Stream(self._client, name=v['name'], tz=v.get('tz', None), exists=True)













def log_progress(sequence, size=0, name='Uploaded'):
    from ipywidgets import IntProgress, HTML, VBox
    from IPython.display import display

    if size <= 200:
        every = 1
    else:
        every = int(size / 200)


    progress = IntProgress(min=0, max=size, value=0)
    label = HTML()
    box = VBox(children=[label, progress])
    display(box)
    index = 0
    ts0 = datetime.utcnow()
    label.value = u'{name}: {index} / {size} Events'.format(
        name=name,
        index=index,
        size=size
    )
    try:
        for record in sequence:
            index += len(record)
            if index == 1 or index % every == 0:
                progress.value = index
                label.value = u'{name}: {index} / {size} Events'.format(
                    name=name,
                    index=index,
                    size=size
                )
            yield record
    except:
        progress.bar_style = 'danger'
        raise
    else:
        ts1 = datetime.utcnow()
        progress.bar_style = 'success'
        progress.value = index
        td = ts1 - ts0
        label.value = "{name} {index} Events in {time}".format(
            name=name,
            index=str(index or '?'),
            time="{:.1f} seconds".format(td.total_seconds()) if td.total_seconds() < 60 else ts
        )








