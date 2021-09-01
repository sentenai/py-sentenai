from sentenai.stream.metadata import Metadata
from sentenai.stream.events import Events, Event
from sentenai.stream.fields import Fields, Field
from sentenai.api import API, iso8601, SentenaiEncoder, PANDAS
if PANDAS:
    import pandas as pd
from datetime import datetime
import simplejson as JSON
import re, io

# Optional for parquet handling
try:
    import pyarrow
except:
    pass


class Streams(API):
    def __init__(self, parent):
        self._parent = parent
        API.__init__(self, parent._credentials, *parent._prefix, "streams")


    if PANDAS:
        def _repr_html_(self):
            data = list(self)
            return pd.DataFrame([{'name': k, 't0': s.t0} for k, s in data])._repr_html_()

    def __repr__(self):
        return "{}.streams".format(repr(self._parent))

    def __getitem__(self, key):
        if type(key) is tuple:
            k, a = key
            return Stream(self, name=k, anchor=a)
        else:
            return Stream(self, name=key)

    def __len__(self):
        return len(self._get().json())

    def __delitem__(self, key):
        x = self[key]
        if x:
            x.delete()
        else:
            raise KeyError("Stream does not exist")

    def __iter__(self):
        return iter([(x['name'], Stream(self, name=x['name'], anchor=x.get('t0'))) for x in self._get().json()])
    
    def __call__(self, name=".*", **kwargs):
        ss = []
        for item in self._get().json():
            if not re.search(name, item['name']):
                continue
            for k, v in kwargs.items():
                if k not in item['meta'] or not re.search(v, item['meta'][k]):
                    break
            else:
                ss.append(item)
        return iter([(x['name'], Stream(self, name=x['name'])) for x in ss])
        
        




class Stream(API):
    def __init__(self, parent, name, filters=None, anchor=None):
        p = {'filters': filters.json()} if filters else {}
        API.__init__(self, parent._credentials, *parent._prefix, name, params=p)
        self._parent = parent
        self._name = name
        self._filters = filters
        self._anchor = anchor

    def init(self, t0="now"):
        if t0 == "now":
            r = self._put(headers={'t0': iso8601(datetime.utcnow())}, json=None)
        elif t0 == None:
            r = self._put()
        else:
            r = self._put(headers={'t0': iso8601(t0)}, json=None)
        if r.status_code != 201:
            raise Exception(r.status_code)

    def upload(self, events):
        r = None
        hdr = {'content-type': 'application/x-ndjson'}
        if isinstance(events, str):
            with open(events) as f:
                r = self._post(json=f.read(), headers=hdr)
        elif isinstance(events, io.IOBase):
            r = self._post(json=events.read(), headers=hdr)
        elif isinstance(events, pyarrow.Table):
            cnames = list(map(str.lower, events.column_names))
            if 'ts' in cnames:
                ts_ix = cnames.index('ts')
            elif 'timestamp' in cnames:
                ts_ix = cnames.index('timestamp')
            else:
                raise ValueError("Needs timestamp column")
            if 'duration' in cnames:
                td_ix = cnames.index('ts')
            else:
                td_ix = None

            def iterate():
                for n in range(len(events)):
                    e = Event(ts=events[ts_ix][n].as_py(), duration=events[td_ix][n].as_py() if td_ix else None, data={})
                    for i, c in enumerate(events.column_names):
                        if i in [td_ix, ts_ix]:
                            continue
                        e.data[events.column_names[i]] = events[i][n].as_py()
                    yield (
                        JSON.dumps({
                            "id": e.id,
                            "ts": e.ts,
                            "duration": e.duration,
                            "event": e.data
                            }, ignore_nan=True, cls=SentenaiEncoder
                        ) + '\n'
                    ).encode()
            r = self._post(json=iterate())
        else:
            r = self._post(json=((JSON.dumps({"id": e.id, "ts": e.ts, "duration": e.duration, "event": e.data}, ignore_nan=True, cls=SentenaiEncoder)+"\n").encode() for e in events), headers=hdr)
        if r.status_code not in range(200, 300):
            raise Exception(r.status_code)

    def __repr__(self):
        return 'Stream(name={!r})'.format(self._name)

    def __delattr__(self, name):
        if name == 'metadata':
            self.metadata.clear()
        else:
            raise TypeError("cannot delete `{}`".format(name))

    def where(self, *filters):
        if len(filters) < 1:
            fs = self._filters
        elif self._filters:
            fs = self._filters & filters[0]
        else:
            fs = filters[0]
        for f in filters[1:]:
            fs &= f
        return Stream(self._parent, self._name, fs, self._anchor)
            
    def json(self):
        d = {'name': self._name}
        if self._anchor:
            d['t0'] = self._anchor
        if 'filters' in self._params:
            d['filter'] = self._params['filters']
        return d

    @property
    def metadata(self):
        return Metadata(self)

    @property
    def events(self):
        return Events(self)

    @property
    def bounds(self):
        try:
            return self.events[::1][0].ts, self.events[::-1][0].ts
        except:
            return (None, None)

    @property
    def t0(self):
        if self._anchor:
            return self._anchor
        else:
            return self._head().headers.get('t0')

    @property
    def name(self):
        return self._name

    def update(self, evt):
        return self.events.update(evt)

    def insert(self, evt):
        return self.events.insert(evt)

    def remove(self, evt):
        return self.events.delete(evt)

    def values(self, at=None):
        params = {}
        if self._anchor:
            params['t0'] = iso8601(self._anchor)
        if at:
            if self.t0:
                params['at'] = iso8601(at)
            else:
                params['at'] = at

        res = self._get(params=params)
        if res.status_code == 200:
            vs = res.json()
            return dict([(tuple(x['path']) if len(x['path']) > 1 else x['path'][0], x['value']) for x in vs])
        elif res.status_code == 404:
            raise ValueError("stream not found")
        else:
            raise Exception(res.status_code)

    def __matmul__(self, ts):
        return self.values(ts)

    def delete(self):
        r = self._delete()
        if r.status_code == 204:
            return None
        else:
            raise Exception("Couldn't delete")

    def __bool__(self):
        r = self._head()
        if r.status_code == 404:
            return False
        elif r.status_code == 200:
            return True
        else:
            raise Exception(r.status_code)

    def __nonzero__(self):
        r = self._head()
        if r.status_code == 404:
            return False
        elif r.status_code == 200:
            return True
        else:
            raise NotImplemented

    def __str__(self):
        return f'stream "{self._name!s}"'

    @property
    def fields(self):
        return Fields(self)

    def __iter__(self):
        return iter(Fields(self))

    def __getitem__(self, key):
        return Fields(self)[(key,) if type(key) != tuple else key]

    def __setitem__(self, key, val):
        raise NotImplemented("What would it do?")




