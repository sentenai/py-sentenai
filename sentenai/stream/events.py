import math
from copy import copy
from datetime import datetime, timedelta
import numpy as np
from sentenai.api import API, dt64, td64, iso8601

import collections

def flatten(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

class Events(API):
    def __init__(self, parent):
        API.__init__(self, parent._credentials, *parent._prefix, "events", params=parent._params)
        self._parent = parent

    def __repr__(self):
        return repr(self._parent) + ".events"

    def __iter__(self):
        i = 0
        n = 100
        while True:
            e = self[i::n]
            if len(e) == 0:
                raise StopIteration
            else:
                for x in e:
                    yield e
                i += n

    def __delitem__(self, i):
        res = self._delete(i)
        if res.status_code == 404:
            raise KeyError("Event does not exist.")
        elif res.status_code != 204:
            raise Exception(res.status_code)

    def __len__(self):
        res = self._head(params=self._params)
        return int(res.headers['events'])

    def __setitem__(self, key, event):
        event.id = key
        self.insert(event)

    def __getitem__(self, i):

        if isinstance(i, str):
            # this is get by id
            res = self._get(i)
            if res.status_code == 200:
                ej = res.json()
                try:
                    ts = int(res.headers['timestamp'])
                except:
                    ts = res.headeres['timestamp']
                return Event(id=i, ts=ts, duration=res.headers.get('duration'), data=ej)
            elif res.status_code == 404:
                raise KeyError("Events does not exist")
            else:
                raise Exception(res.status_code)

        params = copy(self._params)

        if isinstance(i, int):
            raise TypeError("Integer not valid")

        elif isinstance(i, slice):
            # time slice
            t0 = self._parent.t0
            params['sort'] = 'asc'
            if i.start is not None:
                if t0 is None:
                    params['start'] = int(i.start)
                else:
                    params['start'] = iso8601(i.start)
            if i.stop is not None:
                if t0 is None:
                    params['end'] = int(i.stop)
                else:
                    params['end'] = iso8601(i.stop)
            if i.step is not None:
                params['limit'] = abs(i.step)
                if i.step < 0:
                    params['sort'] = 'desc'
            if i.start is not None and i.stop is not None:
                if i.start > i.stop:
                    params['start'], params['end'] = params['end'], params['start']
                    params['sort'] = 'desc'

            resp = self._get(params=params)
            if resp.status_code == 200:
                evts = []
                for ej in resp.json():
                    try:
                        ts = int(ej['ts'])
                    except:
                        ts = ej['ts']
                    evts.append(
                        Event(
                            id=ej['id'],
                            ts=ts,
                            duration=ej.get("duration"),
                            data=ej['event'] or None
                        )
                    )
                return evts
            else:
                raise Exception(resp.status_code)
        else:
            raise ValueError("input must be either string or slice")

    def update(self, evt):
        hdrs = {}
        if evt.id is None:
            raise ValueError("Event id required for updates.")
        if evt.ts is not None:
            hdrs["timestamp"] = iso8601(dt64(evt.ts))
        if evt.duration is not None:
            hdrs["duration"] = str(td64(evt.duration).astype(float) / 1000000000.)
        self._put(evt.id, json=evt.data, headers=hdrs)

    def insert(self, evt):
        hdrs = {'content-type': 'application/json'}
        if evt.ts is not None and evt.duration is None:
            hdrs["timestamp"] = iso8601(evt.ts)
        elif evt.duration is not None:
            hdrs['start'] = iso8601(evt.ts)
            hdrs["end"] = iso8601(evt.ts + evt.duration)

        if evt.id is not None:
            r = self._put(evt.id, json=evt.data, headers=hdrs)
            if r.status_code in [200, 201]:
                return evt
                #return self[r.headers['Location']]
            else:
                raise Exception(r.status_code)
        else:
            r = self._post(json=evt.data, headers=hdrs)
            if r.status_code in [200, 201]:
                return Event(id=r.headers['Location'], data=evt.data, ts=evt.ts, duration=evt.duration)
                #return self[r.headers['Location']]
            else:
                raise Exception(r.status_code)

    def remove(self, evt):
        del self[evt.id]



class Event(object):
    def __init__(self, id=None, ts=None, duration=None, data=None):
        self.id = str(id) if id is not None else None
        self.ts = dt64(ts) if ts is not None else None
        self.duration = td64(duration) if duration is not None else None
        self.data = data

    def as_record(self):
        return flatten({'id': self.id, 'ts': self.ts, 'duration': self.duration, 'event': self.data}, '', '/')

    def __getitem__(self, pth):
        if isinstance(pth, str):
            return self.data[pth]
        else:
            d = self.data
            for s in pth:
                d = d[s]
            return d

    def __repr__(self):
        x = ["{}={}".format(k, repr(getattr(self, k)))
                for k in ("id", "ts", "duration", "data")
                if getattr(self, k) is not None]
        return "Event({})".format(", ".join(x))

    def __len__(self):
        return self.duration

    @property
    def start(self):
        if self.ts and self.duration:
            return self.ts

    @property
    def end(self):
        if self.ts and self.duration:
            return self.ts + self.duration

    def __lt__(self, other):
        if not isinstance(other, Event):
            raise TypeError("Can only compare events.")
        return self.ts < (other.ts or datetime.max) or self.ts == other.ts and self.duration < other.duration

    def __le__(self, other):
        if not isinstance(other, Event):
            raise TypeError("Can only compare events.")
        return self.ts < other.ts or self.ts == other.ts and self.duration <= other.duration

    def __eq__(self, other):
        if not isinstance(other, Event):
            raise TypeError("Can only compare events.")
        return self.ts == other.ts and self.duration == other.duration

    def __gt__(self, other):
        if not isinstance(other, Event):
            raise TypeError("Can only compare events.")
        return self.ts > other.ts or self.ts == other.ts and self.duration > other.duration

    def __ge__(self, other):
        if not isinstance(other, Event):
            raise TypeError("Can only compare events.")
        return self.ts > other.ts or self.ts == other.ts and self.duration >= other.duration

    def __ne__(self, other):
        if not isinstance(other, Event):
            raise TypeError("Can only compare events.")
        return self.ts != other.ts or self.duration != other.duration


