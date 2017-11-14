import json
import re
import requests

import numpy as np
import pandas as pd

from pandas.io.json       import json_normalize
from datetime             import timedelta
from multiprocessing.pool import ThreadPool
from functools            import partial

from sentenai.exceptions import *
from sentenai.exceptions import handle
from sentenai.utils import *
from sentenai.flare import EventPath, Stream, stream, project, ast, delta, Delta

if not PY3:
    import virtualtime
    from Queue import Queue
else:
    from queue import Queue

try:
    from urllib.parse import quote
except:
    from urllib import quote



class Uploader(object):
    def __init__(self, client, iterator, processes=32):
        self.client = client
        self.iterator = iterator
        self.pool = ThreadPool(processes)

    def process(self, data):
        def waits():
            yield 0
            wl = (0,1)
            while True:
                wl = (wl[-1], sum(wl))
                yield wl[-1]

        event = self.validate(data)
        if isinstance(tuple, event):
            return event

        wait = waits()
        while event:
            try:
                self.client.put(**event)
            except AuthenticationError:
                raise
            except APIError as e:
                if e.response.status_code == 400:
                    # probably bad JSON
                    return data
                else:
                    time.sleep(next(wait))
            else:
                return

    def start(self):
        data = self.pool.map(process, self.iterator)
        return { 'saved': len(data), 'failed': filter(data) }


    def validate(self, data):
        ts = data.get('ts')
        try:
            if not ts.tzinfo:
                ts = pytz.localize(ts)
        except:
            return (data, "invalid timestamp")

        sid = data.get('id')
        if sid: sid = str(sid)

        try:
            evt = data['event']
        except KeyError:
            return (data, "missing event data")
        except Exception:
            return (data, "invalid event data")
        else:
            return {"stream": stream(sid), "timestamp": ts, "id": sid}


class Sentenai(object):
    def __init__(self, auth_key="", host="https://api.sentenai.com"):
        self.auth_key = auth_key
        self.host = host
        self.build_url = partial(build_url, self.host)
        self.session = requests.Session()
        self.session.headers.update({ 'auth-key': auth_key })

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
        resp = self.session.delete(url)
        status_codes(resp)


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

        resp = self.session.get(url)

        if resp.status_code == 404 and eid is not None:
            raise NotFound('The event at "/streams/{}/events/{}" does not exist'.format(stream()['name'], eid))
        elif resp.status_code == 404:
            raise NotFound('The stream at "/streams/{}" does not exist'.format(stream()['name'], eid))
        else:
            status_codes(resp)

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
        headers = {'content-type': 'application/json'}
        jd = event

        if timestamp:
            headers['timestamp'] = iso8601(timestamp)

        if id:
            url = '{host}/streams/{sid}/events/{eid}'.format(sid=stream()['name'], host=self.host, eid=id)
            resp = self.session.put(url, json=jd, headers=headers)

            if resp.status_code not in [200, 201]:
                status_codes(resp)
            else:
                return id
        else:
            url = '{host}/streams/{sid}/events'.format(sid=stream._name, host=self.host)
            resp = self.session.post(url, json=jd, headers=headers)
            if resp.status_code in [200, 201]:
                return resp.headers['location']
            else:
                status_codes(resp)
                raise APIError(resp)


    def streams(self, name=None, meta={}):
        """Get list of available streams. Optionally, parameters may be
           supplied to enable searching for stream subsets.

           Optional Arguments:
           name -- A regular expression pattern to search names for
           meta -- A dictionary of key/value pairs to match from stream metadata
        """
        url = "/".join([self.host, "streams"])
        resp = self.session.get(url)
        status_codes(resp)

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
        status_codes(resp)
        return None


    def range(self, stream, start, end):
        """Get all events in stream between start (inclusive) and end (exclusive).

           Arguments:
           stream -- A stream object corresponding to a stream stored in Sentenai.
           start -- A datetime object representing the start of the requested time range.
           end -- A datetime object representing the end of the requested time range.

           Result:
           A time ordered list of all events in a stream from `start` to `end`
        """
        url = "/".join([self.host, "streams", stream()['name'], "start", iso8601(start), "end", iso8601(end)])
        resp = self.session.get(url)
        status_codes(resp)
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


    def fields(self, s):
        if isinstance(s, Stream):
            url = "/".join([self.host, "streams", s['name'], "fields"])
            resp = self.session.get(url)
            return resp.json()
        else:
            raise SentenaiException("Must be called on stream")

    def values(self, s):
        if isinstance(s, Stream):
            url = "/".join([self.host, "streams", s['name'], "values"])
            resp = self.session.get(url)
            return resp.json()
        else:
            raise SentenaiException("Must be called on stream")

    def newest(self, s):
        if isinstance(s, Stream):
            url = "/".join([self.host, "streams", s['name'], "newest"])
            resp = self.session.get(url)
            return {
                    "event": resp.json(),
                    "ts": cts(resp.headers['Timestamp']),
                    "id": resp.headers['Location']
            }
        else:
            raise SentenaiException("Must be called on stream")


    def oldest(self, s):
        if isinstance(s, Stream):
            url = "/".join([self.host, "streams", s['name'], "oldest"])
            resp = self.session.get(url)
            return {
                    "event": resp.json(),
                    "ts": cts(resp.headers['Timestamp']),
                    "id": resp.headers['Location']
            }
        else:
            raise SentenaiException("Must be called on stream")



class Cursor(object):
    def __init__(self, client, query, returning=None, limit=None):
        self.client = client
        self.query = query
        self.returning = returning
        self.limit = limit
        self.headers = {'content-type': 'application/json', 'auth-key': client.auth_key}

        url = '{0}/query'.format(client.host)

        r = handle(requests.post(url, json=ast(query, returning), headers=self.headers))
        self.query_id = r.headers['location']
        self.pool = self._pool()


    def __len__(self):
        return len(self.spans())


    def _pool(self):
        sl = len(self.spans())
        return ThreadPool(16 if sl > 16 else sl) if sl else None


    def _slice(self, cursor, start, end, max_retries=3):
        streams = {}
        retries = 0
        c = "{}+{}Z+{}Z".format(cursor.split("+")[0], start.replace(tzinfo=None).isoformat(), end.replace(tzinfo=None).isoformat())

        while c is not None:
            url = '{host}/query/{cursor}/events'.format(host=self.client.host, cursor=c)
            resp = self.client.session.get(url)

            if not resp.ok and retries >= max_retries:
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
        self.spans()
        pool = self.pool
        if not pool:
            return json.dumps([])
        try:
            data = pool.map(lambda s: self._slice(s['cursor'], s.get('start') or datetime.min, s.get('end') or datetime.max), self._spans)
            return json.dumps(data, default=dts, indent=4)
        finally:
            pool.close()


    def spans(self, refresh=False):
        """Get list of spans of time when query conditions are true."""
        if refresh or not hasattr(self, "_spans"):
            spans = []
            cid = self.query_id
            while cid:
                if self.limit is None:
                    url = '{0}/query/{1}/spans'.format(self.client.host, cid)
                else:
                    url = '{0}/query/{1}/spans?limit={2}'.format(self.client.host, cid, self.limit)
                r = handle(self.client.session.get(url, headers=self.headers)).json()

                for s in r['spans']:
                    if 'start' in s and s['start']:
                        s['start'] = cts(s['start'])
                    #else:
                    #    s['start'] = datetime.min
                    if 'end' in s and s['end']:
                        s['end'] = cts(s['end'])
                    #else:
                    #    s['end'] = datetime.max
                spans.extend(r['spans'])

                cid = r.get('cursor')
                if self.limit and spans >= self.limit:
                    break
            self._spans = spans
        sps = []
        for x in self._spans:
            z = {}
            if 'start' in x:
                z['start'] = x['start']
            if 'end' in x:
                z['end'] = x['end']
            sps.append(z)
        return sps


    def stats(self):
        """Get time-based statistics about query results."""
        self.spans()
        deltas = [sp['end'] - sp['start'] for sp in self._spans if sp.get('start') and sp.get('end')]

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
        """
        The `dataset` method returns the event data from a query.
        It's return type is a "FrameGroup" which can wrap multiple
        dataframes with different shapes. The optional `window` variable
        allows us to specify a window size for each returned slice of
        stream data. The query result can optionally be aligned to the
        LEFT or RIGHT side of the window using the `align` variable. It
        defaults to `CENTER`. When multiple streams have different sample
        rates, it can be handy to specify a `freq` to use. This will engage
        the forward filling capabilities of Pandas to normalize the dataframes.
        """

        if isinstance(window, Delta):
            window = window.timedelta

        def win(cursor, start=datetime.min, end=datetime.max):
            start = start or datetime.min
            end = end or datetime.max
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
            self.spans()
            if not inverted:
                spans = self._spans
            elif self._spans:
                spans = [(datetime.min, self._spans[0][0])]
                for (t0,t1), (u0, u1) in zip(self._spans, self._spans[1:]):
                    spans.append((t1, u0))
            else:
                spans = []

            pool = self._pool()
            for data in pool.map(lambda s: self._slice(*s), [win(**sp) for sp in spans]):
                fr = df(sp, data)
                for s in fr.keys():
                    if fr[s].empty:
                        del fr[s]

                if freq:
                    fr = {k: fr[k].set_index(keys=['.ts'])
                                  .resample(freq).ffill()
                                  .reset_index()
                                  for k in fr}


                for s in fr.keys():
                    fr[s] = fr[s].set_index(keys=['.ts'])
                    fr[s].rename(columns={k: s + ":" + k for k in fr[s].columns}, inplace=True)

                if len(fr.keys()) > 1:
                    to_join = list(fr.values())
                    dff = pd.DataFrame.join(to_join[0], to_join[1:], how="outer").reset_index()
                elif fr:
                    dff = list(fr.values())[0].reset_index()
                else:
                    dff = pd.DataFrame()

                yield dff

        return FrameGroup(iterator)


    def sliding(self, lookback, horizon, slide, freq):
        if isinstance(lookback, Delta):
            lookback = lookback.timedelta
        if isinstance(horizon, Delta):
            horizon = horizon.timedelta
        if isinstance(slide, Delta):
            slide = slide.timedelta

        def slides(start, end):
            cslide = timedelta(0)
            while start + lookback + cslide <= end:
                yield (start + cslide, start + cslide + lookback + horizon)
                cslide += slide

        def shape(inverted):
            self.spans()
            if not inverted:
                spans = self._spans
            elif self._spans:
                spans = [{'cursor': self._spans[0]['cursor'], 'start': "1900-01-01T00:00:00Z", 'end': self._spans[0]['start']}]
                for t0, t1 in zip(self._spans, self._spans[1:]):
                    spans.append({'cursor': t0['cursor'], 'start': t0['end'], 'end': t1['start']})
            rows = 0
            for sp in spans:
                rows += len([x for x in slides(sp['start'], sp['end'])])
            return (rows, len(pd.date_range(t0, t1, freq=freq, closed='right')))

        def iterator(inverted):
            self.spans()
            if not inverted:
                spans = self._spans
            elif self._spans:
                if 'start' in self._spans[0]:
                    spans = [{'cursor': self._spans[0]['cursor'], 'start': "1900-01-01T00:00:00Z", 'end': self._spans[0]['start']}]
                else:
                    spans = []
                for t0, t1 in zip(self._spans, self._spans[1:]):
                    spans.append({'cursor': t0['cursor'], 'start': t0.get('end', datetime.max), 'end': t1.get('start', datetime.min)})
            else:
                spans = []
            for sp in spans:
                start, end, cur = sp.get('start', datetime.min), sp.get('end', datetime.max), sp['cursor']
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
                    if len(p) == len(pd.date_range(t0, t1, freq=freq, closed='right')):
                        yield p

        return FrameGroup(iterator)



class FrameGroup(object):
    def __init__(self, iterator, inverted=False):
        self.iterator = iterator
        self.inverted = inverted

    def inverse(self):
        """
        Return an inverted FrameGroup, by selecting
        the times between the start and end of found
        patterns.
        """
        return FrameGroup(self.iterator, inverted=True)

    def dataframes(self, *columns, **kwargs):
        """
        Return a generator of dataframes with one dataframe per
        found result.
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
            if not df.empty:
                df = df.copy()
                df['.span'] = i
                df['.delta'] = df['.ts'].apply(lambda ts: ts - df['.ts'][0])
                dfs.append(df)
        if dfs:
            rdf = pd.concat(dfs)
            rdf.set_index(['.ts', '.span', '.delta'], inplace=True)
            return rdf
        else:
            return pd.DataFrame()


    def CArray(self, hd5file, group, name, *columns):
        import tables
        t = self.tensor(*columns)
        t.shape
        ds = hd5file.createCArray(group, name, tables.Atom.from_dtype(t.dtype), t.shape)
        ds[:] = t
        hd5file.flush()
        return ds


def df(span, data):
    t0 = span.get('start', datetime.min)
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

