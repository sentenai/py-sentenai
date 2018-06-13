from __future__ import print_function
import json as JSON
import pytz
import re, sys, time
import requests

import numpy as np
import pandas as pd

from pandas.io.json import json_normalize
from datetime import timedelta
from functools import partial
from multiprocessing.pool import ThreadPool
from threading import Lock

from sentenai.exceptions import *
from sentenai.exceptions import handle
from sentenai.utils import *
from sentenai.historiQL import EventPath, Stream, StreamPath

BaseStream = Stream


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



class Event(object):
    def __init__(self, client, stream, id=None, ts=None, event=None, saved=False):
        self.stream = stream
        self.id = id
        self.ts = ts if isinstance(ts, datetime) or ts is None else cts(ts)
        self.event = event
        self._saved = saved

    @property
    def exists(self):
        return bool(self._saved)


    def __repr__(self):
        return "Event({}, {}, saved={})".format(self.stream.name, self.id, self.exists)

    def json(self, include_id=False):
        if include_id:
            return {'ts': self.ts, 'event': self.event, 'id': self.id}
        else:
            return {'ts': self.ts, 'event': self.event}

    def create(self):
        loc = self.stream.put(self.event, self.id, self.ts)
        self.id = loc
        self._saved = True
        return self

    def read(self):
        x = self.stream.read(self.id)
        self.ts = x.ts
        self.event = x.event
        self._saved = True
        return self

    def update(self):
        if not self.id:
            raise Exception("Not found")
        loc = self.stream.put(self.event, self.id, self.ts)
        self.id = loc
        self._saved = True
        return self

    def delete(self):
        self.stream.delete(self.id)
        self._saved = False


class Values(object):
    def __init__(self, stream, at, data):
        self.at = at
        self._data = data

    def __getitem__(self, i):
        if isinstance(i, StreamPath):
            nd = []
            for d in self._data:
                if d['path']._attrlist == i._attrlist:
                    return d['value']
            else:
                raise IndexError
        else:
            return self._data[i]


    def _repr_html_(self):
        df = pd.DataFrame(self._data)
        df['path'] = df['path'].apply(lambda x: ".".join(x._attrlist))
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



class Fields(object):
    def __init__(self, fields):
        self._fields = [f for f in fields if f['path']]

    def __getitem__(self, path):
        xs = []
        for field in self._fields:
            if field._attrlist[:len(path._attrlist)] == path._attrlist:
                xs.append(field)
        return Fields(xs)

    def __repr__(self):
        return repr(self._fields)

    def __iter__(self):
        return iter(self._fields)

    def _repr_html_(self):
        df = pd.DataFrame(sorted(self._fields, key=lambda x: (x['start'], ".".join(x['path']))))
        df['path'] = df['path'].apply(lambda x: ".".join(x))
        df['start'] = df['start'].apply(cts)
        df = df[['path', 'start']]
        return df.rename(
                index=str,
                columns={
                    'path': 'Field',
                    'start': 'Added At'
                    }
            )._repr_html_()


class Stream(BaseStream):
    def __init__(self, client, name, meta, events, tz, *filters):
        self._client = client
        self._events = events
        BaseStream.__init__(self, name, meta, tz, *filters)

    def __len__(self):
        return self._info.get('size', 0)

    def __bool__(self):
        return self._events is not None

    def __len__(self):
        if self._events is None:
            return 0
        else:
            return int(self._events)

    def oldest(self):
        return self._client.oldest(self)
    _oldest = oldest

    def newest(self):
        return self._client.newest(self)
    _newest = newest

    def fields(self):
        return Fields(self._client.fields(self))
    _fields = fields

    def values(self, at=None):
        at = at or datetime.utcnow()
        values = self._client.values(self, at)
        values_rendered = []
        for value in values:
            # create path
            pth = self
            for segment in value['path']:
                pth = pth[segment]
            ts = cts(value['ts'])
            values_rendered.append({
                'timestamp': ts,
                'event': value['id'],
                'value': value['value'],
                'path': pth,
            })
        return Values(self, at, values_rendered)
    _values = values

    def Event(self, *args, **kwargs):
        return Event(self.client, self, *args, **kwargs)
    _Event = Event


    def healthy(self):
        resp = self.session.get("/".join([self.client.host, "streams", self.name]))
        if resp.status_code == 200:
            return resp.json().get('healthy', False)
        elif resp.status_code == 404:
            return None
        else:
            handle(resp)

    _healthy = healthy



    def destroy(self, **kwargs):
        """Delete stream.

        Keyword Argument:
            confirm -- Must be `True` to confirm destroy stream
        """
        return self._client.destroy(self, **kwargs)
    _destroy = destroy

    def delete(self, id):
        """Delete event from the stream by its unique id.

        Arguments:
           eid    -- A unique ID corresponding to an event stored within
                     the stream.
        """
        return self._client.delete(self, id)
    _delete = delete

    def get(self, id):
        """Get event as JSON.

        Arguments:
           eid    -- A unique ID corresponding to an event stored within
                     the stream.
        """
        return self._client.get(self, id)
    _get = get

    def read(self, id):
        """Get event as JSON.

        Arguments:
           eid    -- A unique ID corresponding to an event stored within
                     the stream.
        """
        k = self._client.get(self, id)
        return Event(self._client, self, event=k['event'], id=k['id'], ts=cts(k['ts']), saved=True)
    _read = read

    def fstats(self, field, start=None, end=None):
        """Get stats for a given numeric field.

           Arguments:
           field  -- A dotted field name for a numeric field in the stream.
           start  -- Optional argument indicating start time in stream for calculations.
           end    -- Optional argument indicating end time in stream for calculations.
        """
        return self._client.stats(self, field, start, end)
    _fstats = fstats

    def describe(self, field, start=None, end=None):
        """Describe a given numeric field.

           Arguments:
           field  -- A dotted field name for a numeric field in the stream.
           start  -- Optional argument indicating start time in stream for calculations.
           end    -- Optional argument indicating end time in stream for calculations.
        """
        x = self._client.stats(self, field, start, end)
        if x.get('categorical'):
            print("count\t{count}\nunique\t{unique}\ntop\t{top}\nfreq\t{freq}".format(**x['categorical']))
        else:
            p = x['numerical']
            print("count\t{}\nmean\t{:.2f}\nstd\t{:.2f}\nmin\t{}\n25%\t{}\n50%\t{}\n75%\t{}\nmax\t{}".format(
                p['count'], p['mean'], p['std'], p['min'], p['25%'], p['50%'], p['75%'], p['max']))
    _describe = describe

    def unique(self, field):
        """Get unique values for a given field.

           Arguments:
           field  -- A dotted field name for a numeric field in the stream.
           start  -- Optional argument indicating start time in stream for calculations.
           end    -- Optional argument indicating end time in stream for calculations.
        """
        return self._client.unique(self, field)
    _unique = unique

    def put(self, event, id=None, timestamp=None):
        """Put a new event into this stream.

        Arguments:
           event     -- A JSON-serializable dictionary containing an
                        event's data
           id        -- A user-specified id for the event that is unique to
                        this stream (optional)
           timestamp -- A user-specified datetime object representing the
                        time of the event. (optional)
        """
        return self._client.put(self, event, id, timestamp)
    _put = put

    def range(self, start, end):
        """Get all of a stream's events between start (inclusive) and end (exclusive).

        Arguments:
           start  -- A datetime object representing the start of the requested
                     time range.
           end    -- A datetime object representing the end of the requested
                     time range.

           Result:
           A time ordered list of all events in a stream from `start` to `end`
        """
        return StreamRange(self, start, end, self._client.range(self, start, end))
    _range = range

    def newest(self):
        """Get the most recent event in the stream."""
        return self._client.newest(self)
    _newest = newest

    def oldest(self):
        """Get the oldest event in the stream."""
        return self._client.oldest(self)
    _oldest = oldest


class StreamRange(object):
    def __init__(self, stream, start, end, events):
        self.stream = stream
        self._events = events
        self.start = start
        self.end = end


    def __iter__(self):
        return self._events

    @property
    def df(self):
        f = json_normalize([x.json() for x in self._events])
        return f.set_index('ts')

    def json(self):
        return JSON.dumps([x.json(include_id=True) for x in self._events], default=dts, indent=4)


    def _repr_html_(self):
        return self.df._repr_html_()