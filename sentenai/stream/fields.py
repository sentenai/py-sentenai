from sentenai.api import API, iso8601, dt64, td64
from sentenai.view.expression import Var
from sentenai.pattern.expression import Path

class Fields(API):
    def __init__(self, parent):
        API.__init__(self, parent._credentials, *parent._prefix, params=parent._params)
        self._stream = parent

    def __repr__(self):
        return repr(parent) + ".fields"

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
        else:
            return Field(self, self._stream, key)


class Field(API, Var, Path):
    def __init__(self, parent, stream, *path):
        API.__init__(self, parent._credentials, *parent._prefix, params=parent._params)
        self._path = path
        self._stream = stream

    def __repr__(self):
        return "{}[{}]".format(repr(self._stream), ', '.join(['"{}"'.format(x) for x in self._path]))

    def __str__(self):
        return ".".join(self._path)

    def json(self):
        return {'path': ("event", ) + self._path, 'stream': self._stream.json()}

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
            params = {}
            if s.start is not None:
                try:
                    params['start'] = iso8601(dt64(s.start))
                except:
                    raise TypeError("Range slicing only allowed with datetime types")

            if s.stop is not None:
                try:
                    params['end'] = iso8601(dt64(s.stop))
                except:
                    raise TypeError("Range slicing only allowed with datetime types")

            resp = self._get('stats', *self._path, params=params)
            if resp.status_code == 200:
                data = resp.json()
                return FieldStats(data)
            elif resp.status_code == 404:
                raise ValueError("stream not found")
            else:
                raise Exception(resp.status_code)

        else:
            raise TypeError("Field type can only use range slicing.")

    # shortcuts go get full range field stats
    @property
    def min(self): return self[:].min
    @property
    def max(self): return self[:].max
    @property
    def mean(self): return self[:].mean
    @property
    def std(self): return self[:].std
    @property
    def top(self): return self[:].top
    @property
    def freq(self): return self[:].freq
    @property
    def count(self): return self[:].count
    @property
    def missing(self): return self[:].missing

    @property
    def unique(self):
        resp = self._get("uniques", *self._path)
        if resp.status_code == 200:
            u = resp.json()
            if not u['categorical']:
                return dict(u['numerical'])
            else:
                return dict(u['categorical'])
        else:
            raise Exception(resp.status_code)


class FieldStats(object):
    def __init__(self, stats):
        self._stats = stats

    @property
    def mean(self):
        try:
            return self._stats['numerical']['mean']
        except KeyError:
            raise TypeError

    @property
    def min(self):
        try:
            return self._stats['numerical']['min']
        except KeyError:
            raise TypeError

    @property
    def max(self):
        try:
            return self._stats['numerical']['max']
        except KeyError:
            raise TypeError

    @property
    def std(self):
        try:
            return self._stats['numerical']['std']
        except KeyError:
            raise TypeError

    @property
    def missing(self):
        try:
            if self._stats['numerical']:
                return self._stats['numerical']['missing']
            elif self._stats['categorical']:
                return self._stats['categorical']['missing']
            else:
                raise TypeError
        except KeyError:
            raise TypeError

    @property
    def count(self):
        try:
            if self._stats['numerical']:
                return self._stats['numerical']['count']
            elif self._stats['categorical']:
                return self._stats['categorical']['count']
            else:
                raise TypeError
        except KeyError:
            raise TypeError

    @property
    def top(self):
        try:
            return self._stats['categorical']['top']
        except KeyError:
            raise TypeError

    @property
    def freq(self):
        try:
            return self._stats['categorical']['freq']
        except KeyError:
            raise TypeError
