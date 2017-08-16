import json
import re
import requests

import numpy as np
import pandas as pd

from pandas.io.json       import json_normalize
from datetime             import timedelta
from multiprocessing.pool import ThreadPool
from functools            import partial

from sentenai.exceptions import AuthenticationError, FlareSyntaxError, NotFound, SentenaiException, status_codes, handle
from sentenai.utils import *
from sentenai.flare import EventPath, Stream, stream, project, ast, delta, Delta

try:
    from urllib.parse import quote
except:
    from urllib import quote


class Sentenai(object):
    def __init__(self, auth_key=""):
        self.auth_key = auth_key
        self.host = "https://api.senten.ai"
        self.build_url = partial(build_url, self.host)

    def __str__(self):
        return repr(self)


    def __repr__(self):
        return "Sentenai(auth_key='{}', server='{}')".format(self.auth_key, self.host)


    def debug(self, protocol="http", host="localhost", port=3000):
        self.host = protocol + "://" + host + ":" + str(port)
        return self

    def delete(self, stream, eid):
        """Delete event from a stream by its unique id.

           Arguments:
           stream -- A stream object corresponding to a stream stored in Sentenai.
           eid    -- A unique ID corresponding to an event stored within the stream.
        """
        url = self.build_url(stream, eid)
        headers = {'auth-key': self.auth_key}
        resp = requests.delete(url, headers=headers)
        status_codes(resp.status_code)


    def get(self, stream, eid=None):
        """Get event or stream as JSON.

           Arguments:
           stream -- A stream object corresponding to a stream stored in Sentenai.
           eid    -- A unique ID corresponding to an event stored within the stream.
        """
        if eid:
            url = "/".join([self.host, "streams", stream()['name'], "events", eid])
        else:
            url = "/".join([self.host, "streams", stream()['name']])

        headers = {'auth-key': self.auth_key}
        resp = requests.get(url, headers=headers)

        if resp.status_code == 404 and eid is not None:
            raise NotFound('The event at "/streams/{}/events/{}" does not exist'.format(stream()['name'], eid))
        elif resp.status_code == 404:
            raise NotFound('The stream at "/streams/{}" does not exist'.format(stream()['name'], eid))
        else:
            status_codes(resp.status_code)

        if eid is not None:
            return {'id': resp.headers['location'], 'ts': resp.headers['timestamp'], 'event': resp.json()}
        else:
            return resp.json()


    def put(self, stream, event, id=None, timestamp=None):
        """Put a new event into a stream.

           Arguments:
           stream    -- A stream object corresponding to a stream stored in Sentenai.
           event     -- A JSON-serializable dictionary containing an event's data
           id        -- A user-specified id for the event that is unique to this stream (optional)
           timestamp -- A user-specified datetime object representing the time of the event. (optional)
        """
        headers = {'content-type': 'application/json', 'auth-key': self.auth_key}
        jd = event

        if timestamp:
            headers['timestamp'] = iso8601(timestamp)

        if id:
            url = '{host}/streams/{sid}/events/{eid}'.format(sid=stream()['name'], host=self.host, eid=id)
            resp = requests.put(url, json=jd, headers=headers)
            if resp.status_code not in [200, 201]:
                status_codes(resp.status_code)
                raise SentenaiException("something went wrong")
            else:
                return id
        else:
            url = '{host}/streams/{sid}/events'.format(sid=stream._name, host=self.host)
            resp = requests.post(url, json=jd, headers=headers)
            if resp.status_code in [200, 201]:
                return resp.headers['location']
            else:
                status_codes(resp.status_code)
                raise SentenaiException("something went wrong")


    def streams(self, name=None, meta={}):
        """Get list of available streams. Optionally, parameters may be
           supplied to enable searching for stream subsets.

           Optional Arguments:
           name -- A regular expression pattern to search names for
           meta -- A dictionary of key/value pairs to match from stream metadata
        """
        url = "/".join([self.host, "streams"])
        headers = {'auth-key': self.auth_key}
        resp = requests.get(url, headers=headers)
        status_codes(resp.status_code)

        def filtered(s):
            f = True
            if name:
                f = bool(re.search(name, s['name']))
            for k,v in meta.items():
                f = f and s.get('meta', {}).get(k) == v
            return f

        try:
            return [stream(**v) for v in resp.json() if filtered(v)]
        except:
            raise SentenaiException("Something went wrong")


    def destroy(self, stream):
        """Delete stream

           Argument:
           stream -- A stream object corresponding to a stream stored in Sentenai.
        """
        url = "/".join([self.host, "streams", stream()['name']])
        headers = {'auth-key': self.auth_key}
        resp = requests.delete(url, headers=headers)
        status_codes(resp.status_code)


    def range(self, stream, start, end):
        """Get all events in stream between start (inclusive) and end (exclusive).

           Arguments:
           stream -- A stream object corresponding to a stream stored in Sentenai.
           start -- A datetime object representing the start of the requested time range.
           end -- A datetime object representing the end of the requested time range.

           Result:
           A time ordered list of all events in a stream from `start` to `end`
        """
        url = "/".join([self.host, "streams", stream()['name'], "events", iso8601(start), iso8601(end)])
        headers = {'auth-key': self.auth_key}
        resp = requests.get(url, headers=headers)
        status_codes(resp.status_code)
        return [json.loads(line) for line in resp.text.splitlines()]



    def query(self, query, returning=None, limit=None):
        """Execute a flare query

           Arguments:
           query     -- A query object created via the `select` function.
           limit     -- A limit to the number of result spans returned.
           returning -- An optional dictionary object mapping streams to
                        projections. Each projection is a JSON-serializable
                        dictionary where each value is either a literal
                        (int, bool, float, str) or an EventPath `V.foo`
                        that corresponds to an existing path within the
                        stream's events.
                        example returning dictionary:
                        >>> bos = stream("weather")
                        >>> returning = {
                                bos : {
                                    'high': V.temperatureMax,
                                    'low': V.temperatureMin,
                                    'ccc': {
                                        'foo': 534.2,
                                        'bar': "hello, world!"
                                    }
                                }
                            }
        """
        return Cursor(self, query, returning, limit)


#   def newest(self, o):
#       if isinstance(o, Stream):
#           raise NotImplementedError
#       else:
#           raise SentenaiException("Must be called on stream")


#   def oldest(self, o):
#       if isinstance(o, Stream):
#           raise NotImplementedError
#       else:
#           raise SentenaiException("Must be called on stream")


class FlareResult(object):
    def __init__(self, c, q, spans, ret=None):
        self._client = c
        self._query = q
        self._spans = spans
        self._data = None
        self._window = None
        self._returning = ret

    def spans(self):
        r = []
        for x in self._spans:
            r.append({'start': cts(x['start']), 'end': cts(x['end'])})
        return r

    def stats(self):

        deltas = []
        for sp in self._spans:
            s = cts(sp['start'])
            e = cts(sp['end'])
            deltas.append(e - s)

        if not len(deltas):
            return {}

        mean = sum([3600*24*d.days + d.seconds for d in deltas])/float(len(deltas))
        return {
                'min': min(deltas),
                'max': max(deltas),
                'mean': timedelta(seconds=mean),
                'median': sorted(deltas)[len(deltas)//2],
                'count': len(deltas),
            }

    def json(self):
        if not self._data:
            self._events()
        return json.dumps(self._data, default=dts, indent=4)

    def window(self, width=None, align=CENTER):
        if width is None:
            self._window = None
        else:
            self._window = (width, align)

        if self._data:
            self._data = None

        return self


    def _events(self):
        pool = ThreadPool(8)
        try:
            self._data = pool.map(lambda span: self._slice(cts(span['start']), cts(span['end']), span['cursor']), self._spans)
            return self._data
        finally:
            pool.close()

    def _slice(self, s, e, c):
        if self._window is not None:
            if self._window[1] == LEFT:
                s_ = s
                e_ = s + self._window[0]
            elif self._window[1] == RIGHT:
                s_ = e - self._window[0]
                e_ = e
            else:
                midpoint = cts(s) + (cts(e) - cts(s)) / 2
                s_ = midpoint - self._window[0] / 2
                e_ = midpoint + self._window[0] / 2

            c = "{}+{:%Y-%m-%dT%H:%M:%S}Z+{:%Y-%m-%dT%H:%M:%S}Z".format(c.split("+", 1)[0], s_, e_)

        headers = {'content-type': 'application/json', 'auth-key': self._client.auth_key}
        streams = {}
        retries = 0
        while c is not None:
            url = '{host}/query/{cursor}'.format(host=self._client.host, cursor=c)
            resp = requests.get(url, headers=headers)

            if not resp.ok and retries > 2:
                print(resp)
                raise Exception("failed to get cursor")
            elif not resp.ok:
                retries += 1
                continue
            else:
                retries = 0
                c = resp.headers.get('cursor')
                data = resp.json()

                # using stream_obj var name to avoid clashing with imported
                # stream function from flare.py
                    # initialize stream if it doesn't exist already
                for sid, stream_obj in data['streams'].items():
                    if sid not in streams:
                        streams[sid] = {'stream': stream_obj, 'events': []}

                for event in data['events']:
                    events = streams[event['stream']]['events']
                    ss = streams[event['stream']]['stream']
                    if self._returning is not None and ss in self._returning.keys():
                        evt = {}
                        for key, pth in self._returning[ss].items():
                            evt[key] = event['event']
                            for sg in pth:
                                evt[key] = evt[key][sg]
                        event['event'] = evt
                    del event['stream']
                    events.append(event)

        return {'start': s, 'end': e, 'streams': list(streams.values())}

    def dataframe(self, only=None):

        # call data if not populated
        if not self._data:
            self._events()

        data = self._data
        output = {}

        # return empty frame is no results
        if len(data) < 1:
            if only is not None:
                return pd.DataFrame()
            else:
                return {}

        # loop through streams
        for st in [s['stream'] for s in data[0]['streams']]:

            if only and isinstance(only, Stream):
                if str(only._name) != str(st):
                    continue
            elif only and str(only) != str(st):
                continue

            dd = []
            for x in data:
                for s in x['streams']:
                    if s['stream'] == st:
                        dd.append(s['events'])
                        break

            out = []
            for i, sp in enumerate(dd):
                out_sp = []
                if sp:
                    t0 = cts(sp[0]['ts'])
                for evt in sp:
                    if evt is None: continue
                    ts = cts(evt['ts'])
                    o = {'.stream': st,
                         '.span': i,
                         '.ts': ts,
                         '.delta': ts - t0
                        }

                    for k, v in evt['event'].items():
                        o[k] = v
                    out_sp.append(o)

                if len(out_sp) > 0:
                    df = json_normalize(out_sp)
                    df.set_index(['.ts', '.stream', '.span', '.delta'], inplace=True)
                    out.append(df)
            output[str(st)] = pd.concat(out)
        if only and isinstance(only, Stream):
            return output[str(only._name)]
        elif only:
            return output[only]
        else:
            return output


    def _mIdx(self, df):
        """unused, but we should give the option of multiindexing"""
        midx = pd.MultiIndex.from_tuples(
                        zip(df['.span'], df['.ts'], df['.span']),
                        names=['.span', '.ts', '.span'])

        # FIXME: idx_names is undefined
        # return df.set_index(midx).drop(idx_names, axis=1)
        return



class Cursor(object):
    def __init__(self, client, query, returning=None, limit=None):
        self.client = client
        self.query = query
        self.returning = returning
        self.headers = {'content-type': 'application/json', 'auth-key': client.auth_key}

        if limit is None:
            url = '{0}/query'.format(client.host)
        else:
            url = '{0}/query?limit={1}'.format(client.host, limit)

        # get spans by submitting query to server
        # TODO: Determine if this should be asynchronous
        self._spans = handle(requests.post(url, json=ast(query, returning), headers=self.headers)).json()


    def __len__(self):
        return len(self._spans)


    def _pool(self):
        sl = len(self._spans)
        return ThreadPool(16 if sl > 16 else sl) if sl else None


    def _slice(self, cursor, start, end, max_retries=3):
        streams = {}
        retries = 0
        c = "{}+{:%Y-%m-%dT%H:%M:%S}Z+{:%Y-%m-%dT%H:%M:%S}Z".format(cursor.split("+")[0], start, end)

        while c is not None:
            url = '{host}/query/{cursor}'.format(host=self.client.host, cursor=c)
            resp = requests.get(url, headers=self.headers)

            if not resp.ok and retries >= max_retries:
                print(resp)
                raise Exception("failed to get cursor")
            elif not resp.ok:
                retries += 1
                continue
            else:
                retries = 0
                c = resp.headers.get('cursor')
                data = resp.json()

                # set up stream dict
                for sid, stream_obj in data['streams'].items():
                    if sid not in streams:
                        streams[sid] = {'stream': stream_obj, 'events': []}

                # process each event
                for event in data['events']:
                    events = streams[event['stream']]['events']
                    ss = streams[event['stream']]['stream']
                    del event['stream']
                    events.append(event)
        return {'start': start, 'end': end, 'streams': list(streams.values())}


    def json(self):
        """Get json representation of exact query results."""
        pool = self._pool()
        if not pool:
            return json.dumps([])
        try:
            data = pool.map(lambda s: self._slice(s['cursor'], cts(s['start']), cts(s['end'])), self._spans)
            return json.dumps(data, default=dts, indent=4)
        finally:
            pool.close()


    def spans(self):
        """Get list of spans of time when query conditions are true."""
        r = []
        for x in self._spans:
            r.append({'start': cts(x['start']), 'end': cts(x['end'])})
        return r


    def stats(self):
        """Get time-based statistics about query results."""
        deltas = []
        for sp in self._spans:
            s = cts(sp['start'])
            e = cts(sp['end'])
            deltas.append(e - s)

        if not len(deltas):
            return {}

        mean = sum([3600*24*d.days + d.seconds for d in deltas])/float(len(deltas))
        return {
                'min': min(deltas),
                'max': max(deltas),
                'mean': timedelta(seconds=mean),
                'median': sorted(deltas)[len(deltas)//2],
                'count': len(deltas),
            }


    def dataset(self, window=None, align=CENTER, freq=None):

        def win(cursor, start, end):
            start, end = cts(start), cts(end)
            if window == None:
                return (cursor, start, end)
            if align == LEFT:
                return (cursor, start, start + window)
            elif align == RIGHT:
                return (cursor, end - window, end)
            else:
                mp = start + (end - start) / 2
                w = window / 2
                return (cursor, mp - w, mp + w)

        def iterator(inverted):
            if not inverted:
                spans = self._spans
            elif self._spans:
                spans = [(datetime.min, self.spans[0][0])]
                for (t0,t1), (u0, u1) in zip(self._spans, self._spans[1:]):
                    spans.append((t1, u0))
            else:
                spans = []
            for sp in spans:
                data = self._slice(*win(**sp))
                fr = df(sp, data)
                if freq:
                    fr = {k: fr[k].set_index(keys=['.ts'])
                                  .resample(freq).ffill()
                                  .reset_index()
                                  for k in fr}
                fts = min(fr[k]['.ts'][0] for k in fr)
                lts = max(fr[k]['.ts'][-1] for k in fr) + timedelta(seconds=1)
                for s in fr.keys():
                    fr[s] = fr[s].set_index(keys=['.ts'])
                    fr[s].rename(columns={k: s + ":" + k for k in fr[s].columns}, inplace=True)
                if len(fr.keys()) > 1:
                    to_join = list(fr.values())
                    dff = pd.DataFrame.join(to_join[0], to_join[1:], how="outer").reset_index()
                else:
                    dff = list(fr.values())[0].reset_index()

                yield dff

        return FrameGroup(iterator)


    def sliding(self, lookback, horizon, slide, freq):
        if isinstance(lookback, Delta):
            lookback = lookback.timedelta
        if isinstance(horizon, Delta):
            horizon = horizon.timedelta
        if isinstance(slide, Delta):
            slide = slide.timedelta

        if freq == 'D':
            resolution = timedelta(1)

        def slides(start, end):
            cslide = timedelta(0)
            while start + lookback + cslide <= end:
                yield (start + cslide, start + cslide + lookback + horizon)
                cslide += slide

        def shape(inverted):
            if not inverted:
                spans = self._spans
            elif self._spans:
                spans = [{'cursor': self._spans[0]['cursor'], 'start': "1900-01-01T00:00:00Z", 'end': self._spans[0]['start']}]
                for t0, t1 in zip(self._spans, self._spans[1:]):
                    spans.append({'cursor': t0['cursor'], 'start': t0['end'], 'end': t1['start']})
            rows = 0
            for sp in spans:
                rows += len([x for x in slides(sp['start'], sp['end'])])
            return (rows, divtime(lookback + horizon, freq))

        def iterator(inverted):
            if not inverted:
                spans = self._spans
            elif self._spans:
                spans = [{'cursor': self._spans[0]['cursor'], 'start': "1900-01-01T00:00:00Z", 'end': self._spans[0]['start']}]
                for t0, t1 in zip(self._spans, self._spans[1:]):
                    spans.append({'cursor': t0['cursor'], 'start': t0['end'], 'end': t1['start']})
            else:
                spans = []
            for sp in spans:
                start, end, cur = cts(sp['start']), cts(sp['end']), sp['cursor']
                data = self._slice(cur, start, end + horizon)
                fr = df(sp, data)
                fr = {k: fr[k].set_index(keys=['.ts'])
                              .resample(freq).ffill()
                              .reset_index()
                              for k in fr}
                fts = max(fr[k]['.ts'][0] for k in fr)
                lts = min(fr[k]['.ts'][-1] for k in fr) + timedelta(seconds=1)

                for s in fr.keys():
                    fr[s] = fr[s].set_index(keys=['.ts'])
                    fr[s].rename(columns={k: s + ":" + k for k in fr[s].columns}, inplace=True)

                if len(fr.keys()) > 1:
                    to_join = list(fr.values())
                    dff = pd.DataFrame.join(to_join[0], to_join[1:], how="outer").reset_index()
                else:
                    dff = list(fr.values())[0].reset_index()

                for t0, t1 in slides(fts, lts):
                    p = dff[(dff['.ts'] >= t0) & (dff['.ts'] < t1)]
                    if len(p) == divtime(lookback + horizon, resolution):
                        yield p

        return FrameGroup(iterator)



class FrameGroup(object):
    def __init__(self, iterator, inverted=False):
        self.iterator = iterator
        self.inverted = inverted

    def inverse(self):
        return FrameGroup(self.iterator, inverted=True)

    def dataframes(self, *columns, **kwargs):
        """
        Training set shaped for LSTMs.
        """
        drop_prefixes = kwargs.get('drop_stream_names', False)
        def cname(stream, path):
            return "{}:{}".format(stream['name'], ".".join(path[1:]))

        for df in self.iterator(self.inverted):
            if drop_prefixes:
                # TODO: Figure out what needs to happen if names overlap
                z = df[[cname(**p()) if p != ".ts" else p for p in columns]].copy() if columns else df.copy()
                z.rename(columns={k: k.split(":", 1)[1] for k in z.columns if ":" in k}, inplace=True)
                yield z
            else:
                yield df[[cname(**p()) for p in columns]] if columns else df

    def tensor(self, *columns, **kwargs):
        return np.stack(self.dataframes(*columns, **kwargs))

    def dataframe(self, *columns, **kwargs):
        if columns:
            columns = [".ts"] + list(columns)
        dfs = []
        for i, df in enumerate(self.dataframes(*columns, **kwargs)):
            df = df.copy()
            df['.span'] = i
            df['.delta'] = df['.ts'].apply(lambda ts: ts - df['.ts'][0])
            dfs.append(df)
        rdf = pd.concat(dfs)
        rdf.set_index(['.ts', '.span', '.delta'], inplace=True)
        return rdf


    def CArray(self, hd5file, group, name, *columns):
        import tables
        t = self.tensor(*columns)
        t.shape
        ds = hd5file.createCArray(group, name, tables.Atom.from_dtype(t.dtype), t.shape)
        ds[:] = t
        hd5file.flush()
        return ds


def df(span, data):
    t0 = cts(span['start'])
    dfs = {}
    for s in data['streams']:
        events = []
        for event in s['events']:
            evt = event['event']
            #evt['.id'] = event['id']
            evt['.ts'] = cts(event['ts'])
            #evt['.delta'] = t0 - cts(event['ts'])
            events.append(evt)
        dfs[s['stream']] = json_normalize(events)
    return dfs

