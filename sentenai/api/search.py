import json, re, sys, time, base64
import pytz
import requests

import numpy as np
import pandas as pd


from sentenai.exceptions import *
from sentenai.exceptions import handle
from sentenai.historiQL import Select, Returning
from sentenai.utils import *
from sentenai.api.stream import Stream, Event, TD
from pandas.io.json import json_normalize


class Search(object):
    def __init__(self, client, select, start=None, end=None):
        self.client = client
        self.query = select
        self.start = start
        self.end = end
        self.optimize = True

    def returning(self, *args):
        self.query.statements += (Returning(*args), )
        return self

    def all(self):
        return ResultSet(self).all()

    def resample(self, freq):
        return ResultSet(self).all().resample(freq)

    def __iter__(self):
        return ResultSet(self)

    def __getitem__(self, i):
        return ResultSet(self)[i]

    def head(self, n=5):
        return ResultSet(self)[:n]

    def _spans(self, cursor):
        url = '{0}/query/{1}/spans'.format(self.client.host, cursor)
        retries = 0
        while True:
            try:
                return handle(self.client.session.get(url, params={'optimize': str(self.optimize).lower()})).json()
            except Exception as e:
                retries += 1
                if retries < 3:
                    continue
                else:
                    raise e

    def _repr_html_(self):
        return "<pre>%s</pre>" % str(self.query)

    @property
    def ast(self):
        return self.query()

    def df(self, *args, **kwargs):
        return ResultSet(self).df(*args, **kwargs)

    def agg(self, *args, **kwargs):
        return ResultSet(self).agg(*args, **kwargs)


class ResultPage(object):
    def __init__(self, search, *results, **kwargs):
        self.search = search
        self.results = results
        self.start = kwargs.get("start")
        self.stop = kwargs.get("stop")
        self.freq = None

    def __len__(self):
        return len(self.results) or 0

    def __getitem__(self, i):
        return self.results[i]

    def _repr_html_(self):
        if not self.results: return ""
        maxd = max([
            float(r.duration.total_seconds())
            for r in self.results
            if r.duration is not None and r.duration < timedelta.max])

        x = lambda r: int(round(r.duration.total_seconds() / maxd * 10)) * u"\u2588"
        df = pd.DataFrame([
            { 'Start': r.start,
              'End': r.end,
              'Duration': u"{}".format(r.duration if r.duration else 0),
              'Viz': u"{}".format(x(r)),
            } for r in self.results],
            index=range(self.start or 0, self.stop or (self.start or 0) + len(self.results))
            )
        if df.empty:
            return "_"
        else:
            return df[['Start', 'End', 'Duration', 'Viz']]._repr_html_()

    def df(self, *args, **kwargs):
        dfs = []
        for x in self.results:
            dfs.append(x.df(*args, **kwargs))
        if len(dfs):
            return pd.concat(dfs, keys=range(0,len(dfs)), sort=True)
        else:
            return pd.DataFrame()

    def agg(self, *args, **kwargs):
        dfs = []
        for x in self:
            dfs.append(x.resample(self.freq).agg(*args, **kwargs))
        if len(dfs):
            return pd.concat(dfs, keys=range(0,len(dfs)), sort=True)
        else:
            return pd.DataFrame()


    def resample(self, freq):
        self.freq = freq
        return self


class ResultSet(object):
    def __init__(self, search):
        self.search = search
        r = handle(self.search.client.session.post(
                '{0}/query'.format(search.client.host),
                json = self.search.query(),
                params = {} if search.optimize else {"optimize": "false"}
            ))
        self.cursors = [r.headers['location']]
        self.spans = {}
        self.freq = None
        self.pos = (False, 0)

    def __getitem__(self, i):
        one = type(i) is not slice
        if one: i = slice(i,i+1)
        n = 0
        data = []
        while i.stop is None or n <= i.stop:
            try:
                c = self.cursors[n]
            except IndexError:
                try:
                    next(self)
                except StopIteration:
                    break
            else:
                if c is None and one:
                    raise IndexError
                elif c is None:
                    break
                elif c not in self.spans:
                    try:
                        next(self)
                    except StopIteration:
                        break
                else:
                    n += len(self.spans[c])
        for c in self.cursors:
            if c in self.spans:
                data.extend(self.spans[c])
            else:
                break
        if one:
            return data[0]
        else:
            return ResultPage(self.search, *data[i], start=i.start, stop=i.stop)

    def __iter__(self):
        return self

    def __len__(self):
        return sum([len(x) for x in self.spans.values() if x])

    def __next__(self):
        pg, ps = self.pos
        if pg in self.spans and len(self.spans[pg]) > ps:
            r = self.spans[pg].results[ps]
            self.pos = (pg, ps + 1)
            return r
        elif self.cursors[-1] is None:
            raise StopIteration
        else:
            cc = self.cursors[-1]
            self.pos = (cc, 0)
            r = self.search._spans(cc)
            c = r.get('cursor')
            self.cursors.append(c)
            rp = ResultPage(self.search, *[Result(self.search, **s) for s in r['spans']])
            self.spans[cc] = rp
            return next(self)

    next = __next__


    @property
    def complete(self):
        return self.cursors[-1] is None


    def df(self, *args, **kwargs):
        dfs = []
        for x in self[:]:
            dfs.append(x.df(*args, **kwargs))
        if len(dfs):
            return pd.concat(dfs, keys=range(0,len(dfs)), sort=True)
        else:
            return pd.DataFrame()

    def agg(self, *args, **kwargs):
        f = self.freq if self.freq is not None else '9000AS'
        dfs = []
        for x in self[:]:
            dfs.append(x.resample(f).agg(*args, **kwargs))
        if len(dfs):
            return pd.concat(dfs, keys=range(0,len(dfs)), sort=True)
        else:
            return pd.DataFrame()


    def resample(self, freq):
        self.freq = freq
        return self


    def all(self):
        while self.cursors[-1]:
            if self.cursors[-1] not in self.spans:
                r = self.search._spans(self.cursors[-1])
                c = r.get('cursor')
                rp = ResultPage(self.search, *[Result(self.search, **s) for s in r['spans']])
                self.spans[self.cursors[-1]] = rp
                self.cursors.append(c)
        return self


    def _repr_html_(self):
        data = []
        for cursor in self.cursors:
            data.extend(self.spans.get(cursor, []))
        if data:
            d = [ float(r.duration.total_seconds())
                  for r in data
                  if r.duration is not None]
            maxd = max(d) if d else 1

        x = lambda r: int(round((r.duration.total_seconds() if r.duration else 0) / maxd * 10)) * u"\u2588"
        df = pd.DataFrame([
            { 'Start': r.start,
              'End': r.end,
              'Duration': u"{}".format(r.duration) if r.duration else None,
              'Viz': u"{}".format(x(r)),
            } for r in data])
        if df.empty:
            return "_"
        else:
            return df[['Start', 'End', 'Duration', 'Viz']]._repr_html_()

    def reshape(self, *args, lag, horizon, features):
        lags, hors = [], []
        for r in list(self[:]):
            if not lags:
                lags.append((self.search.start or DTMIN, r.start))
            else:
                lags.append((hors[-1][1], r.start))

            hors.append((r.start, r.end))
        else:
            if hors and hors[-1][1]:
                lags.append((hors[-1][1], self.search.end or DTMAX))

        streams = {}
        for i, feature in enumerate(features):
            if feature.stream:
                z = streams.get(feature.stream, {})
                if not z:
                    streams[feature.stream] = z
                z['feature-{:04d}'.format(i)] = feature

        kwargs = dict(("feature-{:04d}".format(i), arg) for i, arg in enumerate(features))
        keys = list(sorted(kwargs.keys()))

        ps = [s @ p for s, p in streams.items()]

        if self.freq is None:
            raise Exception("Cannot call `.reshape()` on raw data.")

        def tensors():
            for i, (s, e) in enumerate(lags):
                r = Result(self.search, s, e, "{}+{}+{}".format(self.cursors[0], iso8601(s), iso8601(e)).replace("+00:00", "Z"))
                a = r.resample(self.freq).agg(*ps)
                a['span'] = 0

                if i < len(hors):
                    r2 = Result(self.search, s, e, "{}+{}+{}".format(self.cursors[0], iso8601(hors[i][0]), iso8601(hors[i][1])).replace("+00:00", "Z")).limit(horizon)
                    b = r2.resample(self.freq).agg(*ps)
                    b['span'] = 1
                    z = pd.concat([a,b])
                else:
                    z = a
                d1, d2 = [], []
                for i, row in z.iterrows():
                    d1.append([row[k] for k in keys])
                    d2.append(row['span'])

                for i in range(len(z) - lag - horizon):
                    yield ( np.array([[x for x in d1[i:i+lag]]], np.float32)
                          , np.array(d2[i+lag:i+lag+horizon], np.float32)
                          )

        return TD(tensors())

class Result(object):
    def __init__(self, search, start, end, cursor):
        self.search = search
        self.start = cts(start) if start else None
        self.end = cts(end) if end else None
        self.events = 0
        self.cursor = cursor
        self.projection = None
        self.freq = None
        self._limit = None


    @property
    def duration(self):
        if self.end and self.start:
            return self.end - self.start
        else:
            return None

    def limit(self, n):
        self._limit = n
        return self

    def _repr_html_(self):
        df = pd.DataFrame([
            { 'Start': self.start,
              'End': self.end,
              'Duration': u"{}".format(self.duration if self.duration else 0),
            }])
        if df.empty:
            return "_"
        else:
            return df[['Start', 'End', 'Duration']]._repr_html_()

    def _count(self, max_retries=1):
        if max_retries < 0:
            raise SentenaiException("Max Retries Exceeded.")
        streams = {}
        url = '{host}/query/{cursor}/events'.format(host=self.search.client.host, cursor=self.cursor)
        try:
            r = self.search.client.session.get(url, params={'limit': '0'})
            if not r.ok:
                if max_retries > 0:
                    return self._count(max_retries - 1)
                else:
                    raise SentenaiException(r.status_code)
        except:
            if max_retries > 0:
                return self._count(max_retries - 1)
            else:
                raise
        else:
            self._json = r.json()
            return 0

    def _events(self, max_retries=3):
        if max_retries < 0:
            raise SentenaiException("Max Retries Exceeded.")
        streams = {}
        url = '{host}/query/{cursor}/events'.format(host=self.search.client.host, cursor=self.cursor)
        if self.projection:
            params = {'projections': base64.urlsafe_b64encode(bytes(json.dumps(self.projection()), 'UTF-8'))}
        else:
            params = {}
        if self._limit:
            params['limit'] = str(self._limit)

        try:
            r = self.search.client.session.get(url, params=params)
            if not r.ok:
                if max_retries > 0:
                    return self._events(max_retries - 1)
                else:
                    raise SentenaiException(r.status_code)
        except:
            if max_retries > 0:
                return self._events(max_retries - 1)
            else:
                raise
        else:
            self._json = r.json()
            return self._json

    def json(self, *attrs):
        if attrs and self.projection:
            self.projection = Returning(*args)
            return self._events()
        elif not attrs and not self.projection:
            try:
                return self._json
            except:
                return self._events()
        else:
            self.projection = Returning(*attrs) if attrs else None
            return self._events()

    def df(self, *attrs, **kwargs):
        if not attrs:
            if self.projection:
                self._json = None
            self.projection = None
        else:
            self._json = None
            if len(attrs) > 0:
                kwargs['default'] = kwargs.get('default', False)
            self.projection = Returning(*attrs, **kwargs)
        x = json_normalize([evt.json(df=True) for evt in iter(self)])
        return x if x.empty else x.set_index('ts')


    def agg(self, *attrs, **kwargs):
        self.projection = Returning(*attrs, frequency=self.freq, join=True)
        data = self._events()
        els = []
        ss = {}
        for e in data['events']:
            el = {'ts': e['ts']}
            el.update(e['event'])
            if e['stream'] not in ss:
                ss[e['stream']] = []
            ss[e['stream']].append(el)
        dfs = [pd.DataFrame(ss[v]) for v in ss]
        for df in dfs:
            if not df.empty:
                df.set_index('ts', inplace=True)
        if len(dfs) == 0:
            out = pd.DataFrame()
        elif len(dfs) == 1:
            out = dfs[0]
        else:
            out = dfs[0].join(dfs[1:], how='outer').ffill()
        if self.freq == '9000AS':
            out.reset_index(inplace=True)
            del out['ts']
            out['start'] = self.start
            out['end'] = self.end
            out['duration'] = self.duration
            return out.set_index(['start', 'end'])
        else:
            return out


    def resample(self, freq):
        self.freq = freq
        return self


    @property
    def streams(self):
        data = self.json()
        if not data:
            return []
        else:
            return [Stream(self.search.client, s['name'], {}, None, True)
                    for s in data.get('streams', []).values()]


    def __iter__(self):
        data = self._events()
        try:
            streams = {k: Stream(self.search.client, s['name'])
                       for k, s in data.get('streams', {}).items()}
        except KeyError:
            # TEMPORARY: REMOVE AFTER FIX
            streams = {k: Stream(self.search.client, s['stream']['name'])
                       for k, s in data.get('streams', {}).items()}
        return iter([Event(self.search.client, streams[e['stream']], e['id'], e['ts'], e['event'] or {})
                     for e in data['events']])

    def __getitem__(self, i):
        ss = []
        if type(i) is tuple:
            for s in self.streams:
                if s.id in i:
                    ss.append(s)
        else:
            for s in self.streams:
                if s.id == i:
                    ss.append(s)
        return RView(self, ss)


class RView(object):
    def __init__(self, result, streams, head=None):
        self.result = result
        self.streams = streams
        self._head = head

    def _view(self):
        data = self.result.json()
        rdict = {k: [] for k in self.streams}
        sdict = {k: Stream(self.result.search.client, s['name'], {}, {}, None, True)
                   for k, s in data.get('streams', {}).items()}
        for e in data['events']:
            st = sdict.get(e['stream'])
            if st in self.streams:
                if self._head is None or len(rdict[st]) < self._head:
                    rdict[st].append(Event(self.result.search.client, st, e['id'], e['ts'], e['event']))
        return rdict

    def _repr_html_(self):
        rd = self._view()
        vs = []
        for k, v in rd.items():
            vs.append("<div><b>{}</b></div>".format(k.id) + json_normalize([x.json(df=True) for x in v])._repr_html_())

        return "<hr/>".join(vs)

    def head(self, n=20):
        return RView(self.result, self.streams, n)

    @property
    def df(self):
        return json_normalize([x.json(df=True) for x in v])


