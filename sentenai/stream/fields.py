from sentenai.api import API, iso8601, dt64, td64, PANDAS
from sentenai.view.expression import Var
from sentenai.view.views import Views
from sentenai.pattern.expression import Path
if PANDAS: import pandas as pd

class Fields(API):
    def __init__(self, parent):
        API.__init__(self, parent._credentials, *parent._prefix, params=parent._params)
        self._stream = parent

    def __repr__(self):
        return repr(self._stream) + ".fields"

    if PANDAS:
        def _repr_html_(self):
            return pd.DataFrame([
                {'field': str(f)} for f in iter(self)
            ])._repr_html_()

    def __iter__(self):
        x = self._get("fields")
        if x.status_code == 200:
            return iter(Field(self, self._stream, *f['path']) for f in x.json())
        elif x.status_code == 404:
            raise ValueError("stream not found.")
        else:
            raise Exception(x.status_code)

    def __getitem__(self, key):
        if type(key) == tuple:
            return Field(self, self._stream, *key)
        elif type(key) == slice:
            if key.start is None and key.stop == 'ts':
                return TimestampField(self, self._stream)
            elif key.start is None and key.stop == 'duration':
                return DurationField(self, self._stream)
            else:
                raise ValueError("invalid path")
        else:
            return Field(self, self._stream, key)

class FieldData(object):
    def __init__(self, stream, do):
        self._str = stream
        self._do = do

    def __getitem__(self, s):
        if isinstance(s, slice) and self._str.t0 and (s.start is None or s.end is None):
            t0, t1 = self._str.bounds
            s = slice(s.start or t0, s.stop or t1, s.step)
        return self._do[s]







class Field(API, Var, Path):
    def __init__(self, parent, stream, *path, **kwargs):
        API.__init__(self, parent._credentials, *parent._prefix, params=parent._params)
        self._parent = parent
        self._path = path
        self._shift = None
        self._stream = stream
        self._start = kwargs.get('start')
        self._end = kwargs.get('end')

    def __repr__(self):
        return "{}[{}]".format(repr(self._stream), ', '.join(['"{}"'.format(x) for x in self._path]))

    def __str__(self):
        return "/".join(self._path)

    @property
    def data(self):
        d = Views(API(self._credentials))(value=self).data
        return FieldData(self._stream, d)


    def json(self):
        d = {'path': ("event", ) + self._path, 'stream': self._stream.json()}
        if self._shift:
            d['shift'] = self._shift
        return d

    @property
    def path(self):
        return tuple(self._path)

    def __matmul__(self, ts):
        return self.value(at=ts)

    def value(self, at=None):
        res = self._get("values", *self._path, params={'at': iso8601(at)} if at is not None else {})
        if res.status_code == 200:
            return res.json()['value']
        elif res.status_code == 404:
            raise ValueError("stream not found")
        else:
            raise Exception(res.status_code)

    def __getitem__(self, s):
        if type(s) == slice:
            return Field(self._parent, self._stream, *self._path, start=s.start, end=s.stop)

            params = {}
            if s.start is not None:
                try:
                    params['start'] = iso8601(dt64(s.start))
                except:
                    raise ValueError("Range slicing only allowed with datetime types")

            if s.stop is not None:
                try:
                    params['end'] = iso8601(dt64(s.stop))
                except:
                    raise ValueError("Range slicing only allowed with datetime types")



        else:
            raise ValueError("Field type can only use range slicing.")

    @property
    def stats(self): 
        params = {}
        if self._start:
            params['start'] = self._start
        if self._end:
            params['end'] = self._end
        if self._stream._filters:
            params['filters'] = self._stream._filters.json()

        resp = self._get('stats', *self._path, params=params)
        if resp.status_code == 200:
            data = resp.json()
            return FieldStats(data)
        elif resp.status_code == 404:
            raise ValueError("stream not found")
        else:
            raise Exception(resp.status_code)

    # shortcuts go get full range field stats
    @property
    def min(self): return self.stats.min
    @property
    def max(self): return self.stats.max
    @property
    def mean(self): return self.stats.mean
    @property
    def std(self): return self.stats.std
    @property
    def top(self): return self.stats.top
    @property
    def freq(self): return self.stats.freq
    @property
    def count(self): return self.stats.count
    @property
    def missing(self): return self.stats.missing

    @property
    def unique(self):
        params = {}
        if self._start:
            params['start'] = self._start
        if self._end:
            params['end'] = self._end
        params['filters'] = self._stream._filters.json() if self._stream._filters else None
        resp = self._get("uniques", *self._path, params=params)
        if resp.status_code == 200:
            u = resp.json()
            if not u['categorical']:
                return {float(v) for v in dict(u['numerical']).keys()}
            else:
                return set(dict(u['categorical']).keys())
        else:
            raise Exception(resp.status_code)

class TimestampField(Field):
    def json(self):
        d = {'path': ("ts", ) + self._path, 'stream': self._stream.json()}
        if self._shift:
            d['shift'] = self._shift
        return d

class DurationField(Field):
    def json(self):
        d = {'path': ("duration", ) + self._path, 'stream': self._stream.json()}
        if self._shift:
            d['shift'] = self._shift
        return d



class FieldStats(object):
    def __init__(self, stats):
        self._stats = stats

    if PANDAS:
        def _repr_html_(self):
            return pd.DataFrame([
                {'Statistic': k, 'Value': v}
                for k, v in self._stats[
                    'numerical'
                    if self._stats.get('numerical')
                    else 'categorical'
                ].items()
            ])._repr_html_()


    @property
    def mean(self):
        try:
            return self._stats['numerical']['mean']
        except KeyError:
            return None

    @property
    def min(self):
        try:
            return self._stats['numerical']['min']
        except KeyError:
            return None

    @property
    def max(self):
        try:
            return self._stats['numerical']['max']
        except KeyError:
            return None

    @property
    def std(self):
        try:
            return self._stats['numerical']['std']
        except:
            return None

    @property
    def missing(self):
        try:
            if self._stats['numerical']:
                return self._stats['numerical']['missing']
            elif self._stats['categorical']:
                return self._stats['categorical']['missing']
            else:
                return None
        except KeyError:
            return None

    @property
    def count(self):
        try:
            if self._stats['numerical']:
                return self._stats['numerical']['count']
            elif self._stats['categorical']:
                return self._stats['categorical']['count']
            else:
                return None
        except KeyError:
            return None

    @property
    def top(self):
        try:
            return self._stats['categorical']['top']
        except KeyError:
            return None

    @property
    def freq(self):
        try:
            return self._stats['categorical']['freq']
        except KeyError:
            return None
