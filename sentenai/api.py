from __future__ import print_function
import json
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
from sentenai.flare import EventPath, Stream, stream, delta, Delta, Query

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
        self.succeeded = 0
        self.failed = 0
        self.lock = Lock()

    def start(self, progress=False):
        def process(data):
            def waits():
                yield 0
                wl = (0,1)
                while True:
                    wl = (wl[-1], sum(wl))
                    yield wl[-1]

            event = self.validate(data)
            if isinstance(event, tuple):
                return event

            wait = waits()
            while event:
                try:
                    self.client.put(**event)
                    with self.lock:
                        self.succeeded += 1
                    return None
                except AuthenticationError:
                    raise
                except Exception as e:
                    w = next(wait)
                    if w < 15: # 15 second wait limit
                        time.sleep(next(wait))
                    else:
                        with self.lock:
                            self.failed += 1
                        return (event, e)
                    """
                    if e.response.status_code == 400:
                        # probably bad JSON
                        return data
                    else:
                        time.sleep(next(wait))
                    """
        if progress:
            events = list(self.iterator)
            total  = len(events)
            def bar():
                sys.stderr.write("\r" * 60)
                sc = int(round(50 * self.succeeded / float(total)))
                fc = int(round(50 * self.failed    / float(total)))
                pd = (self.failed + self.succeeded) / float(total) * 100.
                sys.stderr.write(" [\033[92m{0}\033[91m{1}\033[0m{2}] {3:>6.2f}%   ".format( "#" * sc, "#" * fc, " " * (50 - sc - fc), pd))
                sys.stderr.flush()

            t0 = datetime.utcnow()
            data = self.pool.map_async(process, events)
            #sys.stderr.write("\n " + "-" * 62 + " \n")
            sys.stderr.write("\n {:<60} \n".format("Uploading {} objects:".format(total)))
            while not data.ready():
                bar()
                time.sleep(.1)
            else:
                bar()
            t1 = datetime.utcnow()
            sys.stderr.write("\n {:<60} ".format("Time elapsed: {}".format(t1 - t0)))
            sys.stderr.write("\n {:<60} ".format("Mean Obj/s: {:.1f}".format(float(total)/(t1 - t0).total_seconds())))
            sys.stderr.write("\n {:<60} \n\n".format("Failures: {}".format(self.failed)))
            #sys.stderr.write("\n " + "-" * 62 + " \n\n")
            sys.stderr.flush()
            #data.wait()
            data = data.get()
        else:
            data = self.pool.map(process, list(self.iterator))
        return { 'saved': len(data), 'failed': filter(None, data) }


    def validate(self, data):
        ts = data.get('ts')
        try:
            if not ts.tzinfo:
                ts = pytz.utc.localize(ts)
        except:
            return (data, "invalid timestamp")

        sid = data.get('stream')
        if isinstance(sid, Stream):
            strm = sid
        elif sid:
            strm = stream(str(sid))
        else:
            return (data, "missing stream")

        try:
            evt = data['event']
        except KeyError:
            return (data, "missing event data")
        except Exception:
            return (data, "invalid event data")
        else:
            return {"stream": strm, "timestamp": ts, "id": data.get('id'), "event": evt}


class Sentenai(object):
    def __init__(self, auth_key="", host="https://api.sentenai.com"):
        """Initialize a Sentenai client.

        The client object handles all requests to the Sentenai API.

        Arguments:
            auth_key -- a Sentenai API auth key
        """
        self.auth_key = auth_key
        self.host = host
        self.build_url = partial(build_url, self.host)
        self.session = requests.Session()
        self.session.headers.update({ 'auth-key': auth_key })


    def upload(self, iterable, processes=4, progress=False):
        """Takes a list of events and creates an instance of a Bulk uploader.

        Arguments:
            iterable -- an iterable object or list of events with each
                        event in this format:
                { "stream": Stream("foo) or "foo",
                  "id": "my-unique-id" (optional),
                  "ts": "2000-10-10T00:00:00Z",
                  "event": {<<event body>>}
                }
            processes -- number of processes to use. Too many processes might
                         cause a slowdown in upload speed.

        The Uploader object returned needs to be triggered with its `.start()`
        method.

        """
        ul = Uploader(self, iterable, processes)
        return ul.start(progress)


    def __str__(self):
        """Return a string representation of the object."""
        return repr(self)


    def __repr__(self):
        """Return an unambiguous representation of the object."""
        return "Sentenai(auth_key='{}', server='{}')".format(
            self.auth_key, self.host)


    def debug(self, protocol="http", host="localhost", port=3000):
        self.host = protocol + "://" + host + ":" + str(port)
        return self


    def delete(self, stream, eid):
        """Delete event from a stream by its unique id.

        Arguments:
           stream -- A stream object corresponding to a stream stored
                     in Sentenai.
           eid    -- A unique ID corresponding to an event stored within
                     the stream.
        """
        url = self.build_url(stream, eid)
        resp = self.session.delete(url)
        status_codes(resp)

    def get(self, stream, eid=None):
        """Get event or stream as JSON.

        Arguments:
           stream -- A stream object corresponding to a stream stored
                     in Sentenai.
           eid    -- A unique ID corresponding to an event stored within
                     the stream.
        """
        if eid:
            url = "/".join(
                [self.host, "streams", stream()['name'], "events", eid])
        else:
            url = "/".join([self.host, "streams", stream()['name']])

        resp = self.session.get(url)

        if resp.status_code == 404 and eid is not None:
            raise NotFound(
                'The event at "/streams/{}/events/{}" '
                'does not exist'.format(stream()['name'], eid))
        elif resp.status_code == 404:
            raise NotFound(
                'The stream at "/streams/{}" '
                'does not exist'.format(stream()['name'], eid))
        else:
            status_codes(resp)

        if eid is not None:
            return {
                'id': resp.headers['location'],
                'ts': resp.headers['timestamp'],
                'event': resp.json()}
        else:
            return resp.json()

    def stats(self, stream, field, start=None, end=None):
        """Get stats for a given field in a stream.

           Arguments:
           stream -- A stream object corresponding to a stream stored in Sentenai.
           field  -- A dotted field name for a numeric field in the stream.
           start  -- Optional argument indicating start time in stream for calculations.
           end    -- Optional argument indicating end time in stream for calculations.
        """
        args = {}
        if start: args['start'] = start.isoformat() + ("Z" if not start.tzinfo else "")
        if end: args['end'] = end.isoformat() + ("Z" if not end.tzinfo else "")

        url = "/".join([self.host, "streams", stream()['name'], "fields", field, "stats"])

        resp = self.session.get(url, params=args)

        if resp.status_code == 404:
            raise NotFound('The field at "/streams/{}/fields/{}" does not exist'.format(stream()['name'], field))
        else:
            status_codes(resp)

        return resp.json()

    def put(self, stream, event, id=None, timestamp=None):
        """Put a new event into a stream.

        Arguments:
           stream    -- A stream object corresponding to a stream stored
                        in Sentenai.
           event     -- A JSON-serializable dictionary containing an
                        event's data
           id        -- A user-specified id for the event that is unique to
                        this stream (optional)
           timestamp -- A user-specified datetime object representing the
                        time of the event. (optional)
        """
        headers = {
            'content-type': 'application/json'
        }
        jd = event

        if timestamp:
            headers['timestamp'] = iso8601(timestamp)

        if id:
            url = '{host}/streams/{sid}/events/{eid}'.format(
                sid=stream()['name'], host=self.host, eid=id
            )
            resp = self.session.put(url, json=jd, headers=headers)
            if resp.status_code not in [200, 201]:
                status_codes(resp)
            else:
                return id
        else:
            url = '{host}/streams/{sid}/events'.format(
                sid=stream._name, host=self.host
            )
            resp = self.session.post(url, json=jd, headers=headers)
            if resp.status_code in [200, 201]:
                return resp.headers['location']
            else:
                status_codes(resp)
                raise APIError(resp)

    def streams(self, name=None, meta={}):
        """Get list of available streams.

        Optionally, parameters may be supplied to enable searching
        for stream subsets.

        Arguments:
           name -- A regular expression pattern to search names for
           meta -- A dictionary of key/value pairs to match from stream
                   metadata
        """
        url = "/".join([self.host, "streams"])
        resp = self.session.get(url)
        status_codes(resp)

        def filtered(s):
            f = True
            if name:
                f = bool(re.search(name, s['name']))
            for k, v in meta.items():
                f = f and s.get('meta', {}).get(k) == v
            return f

        try:
            return [stream(**v) for v in resp.json() if filtered(v)]
        except:
            raise SentenaiException("Something went wrong")

    def destroy(self, stream):
        """Delete stream.

        Argument:
           stream -- A stream object corresponding to a stream stored
                     in Sentenai.
        """
        url = "/".join([self.host, "streams", stream()['name']])
        headers = {'auth-key': self.auth_key}
        resp = requests.delete(url, headers=headers)
        status_codes(resp)
        return None

    def range(self, stream, start, end):
        """Get all stream events between start (inclusive) and end (exclusive).

        Arguments:
           stream -- A stream object corresponding to a stream stored
                     in Sentenai.
           start  -- A datetime object representing the start of the requested
                     time range.
           end    -- A datetime object representing the end of the requested
                     time range.

           Result:
           A time ordered list of all events in a stream from `start` to `end`
        """
        url = "/".join(
            [self.host, "streams",
             stream()['name'],
             "start",
             iso8601(start),
             "end",
             iso8601(end)]
        )
        resp = self.session.get(url)
        status_codes(resp)
        return [json.loads(line) for line in resp.text.splitlines()]

    def query(self, *statements, **kwargs):
        """Execute a flare query.

        Arguments:
           *statements -- Includes query objects created via the `select`
                          function and projections created via `returning`.
           limit     -- A limit to the number of result spans returned.
        """
        return Cursor(self, Query(*statements), limit=kwargs.get('limit', None))

    def fields(self, stream):
        """Get a list of field names for a given stream

        Argument:
           stream -- A stream object corresponding to a stream stored
                     in Sentenai.
        """
        if isinstance(stream, Stream):
            url = "/".join([self.host, "streams", stream['name'], "fields"])
            resp = self.session.get(url)
            status_codes(resp)
            return resp.json()
        else:
            raise SentenaiException("Must be called on stream")

    def values(self, stream):
        """Get all the latest values for a given stream.

        If the events in the stream don't share all their fields, this will
        forward fill values, returning the latest value for every field seen
        in the stream.

        Argument:
           stream -- A stream object corresponding to a stream stored
                     in Sentenai.
        """
        if isinstance(stream, Stream):
            url = "/".join([self.host, "streams", stream['name'], "values"])
            resp = self.session.get(url)
            status_codes(resp)
            return resp.json()
        else:
            raise SentenaiException("Must be called on stream")

    def newest(self, stream):
        """Get the most recent event in a given stream.

        Argument:
           stream -- A stream object corresponding to a stream stored
                     in Sentenai.
        """
        if isinstance(stream, Stream):
            url = "/".join([self.host, "streams", stream['name'], "newest"])
            resp = self.session.get(url)
            status_codes(resp)
            return {
                    "event": resp.json(),
                    "ts": cts(resp.headers['Timestamp']),
                    "id": resp.headers['Location']
            }
        else:
            raise SentenaiException("Must be called on stream")


    def oldest(self, stream):
        """Get the oldest event in a given stream.

        Argument:
           stream -- A stream object corresponding to a stream stored
                     in Sentenai.
        """
        if isinstance(stream, Stream):
            url = "/".join([self.host, "streams", stream['name'], "oldest"])
            resp = self.session.get(url)
            status_codes(resp)
            return {
                    "event": resp.json(),
                    "ts": cts(resp.headers['Timestamp']),
                    "id": resp.headers['Location']
            }
        else:
            raise SentenaiException("Must be called on stream")


class Cursor(object):
    def __init__(self, client, query, limit=None):
        self.client = client
        self.query = query
        self._limit = limit
        self.headers = {'content-type': 'application/json', 'auth-key': client.auth_key}

        url = '{0}/query'.format(client.host)

        r = handle(requests.post(url, json=self.query(), headers=self.headers))
        self.query_id = r.headers['location']
        self._pool = None


    def __len__(self):
        return len(self.spans())

    @property
    def pool(self):
        if self._pool:
            return self._pool
        else:
            sl = len(self.spans())
            self._pool = ThreadPool(16 if sl > 16 else sl) if sl else None
            return self._pool

    def _slice(self, cursor, start, end, max_retries=3):
        """Slice a set of spans and events.

        TODO: Add descriptions

        Arguments:
            cursor      --
            start       --
            end         --
            max_retries --
        """
        streams = {}
        retries = 0
        c = "{}+{}Z+{}Z".format(
            cursor.split("+")[0],
            start.replace(tzinfo=None).isoformat(),
            end.replace(tzinfo=None).isoformat()
        )

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

                # using stream_obj var name to avoid clashing with imported
                # stream function from flare.py
                # initialize stream if it doesn't exist already
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
        """Return query results as a JSON string.

        Returns:
            json_data -- A JSON string of query results.
                         [{
                            'start': The start timestamp of the span,
                            'end': The end timestamp of the span,
                            'streams': [
                                {
                                    'stream': stream name,
                                    'events': [list of matching events]
                                }
                            ]
                         },
                          ...]
        """
        self.spans()
        pool = self.pool
        if not pool:
            return json.dumps([])
        try:
            data = pool.map(lambda s: self._slice(s['cursor'], s.get('start') or DTMIN, s.get('end') or DTMAX), self._spans)
            return json.dumps(data, default=dts, indent=4)
        finally:
            pool.close()

    def dataframe(self, *args, **kwargs):
        return self.dataset().dataframe(*args, **kwargs)

    def spans(self, refresh=False):
        """Get list of spans of time when query conditions are true."""
        if refresh or not hasattr(self, "_spans"):
            spans = []
            cid = self.query_id
            while cid:
                if self._limit is None:
                    url = '{0}/query/{1}/spans'.format(self.client.host, cid)
                else:
                    url = '{0}/query/{1}/spans?limit={2}'.format(self.client.host, cid, self._limit)
                r = handle(self.client.session.get(url, headers=self.headers)).json()

                for s in r['spans']:
                    if 'start' in s and s['start']:
                        s['start'] = cts(s['start'])
                    if 'end' in s and s['end']:
                        s['end'] = cts(s['end'])
                spans.extend(r['spans'])

                cid = r.get('cursor')
                if self._limit and len(spans) >= self._limit:
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

        mean = sum([3600*24*d.days + d.seconds for d in deltas]) / float(len(deltas))
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

        def win(cursor, start=DTMIN, end=DTMAX):
            start = start or DTMIN
            end = end or DTMAX
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
                spans = [(DTMIN, self._spans[0][0])]
                for (t0,t1), (u0, u1) in zip(self._spans, self._spans[1:]):
                    spans.append((t1, u0))
            else:
                spans = []

            pool = self.pool
            for start, data in pool.map(lambda s: (s[1], self._slice(*s)), [win(**sp) for sp in spans]):
                fr = df(start, data)
                for s in fr.keys():
                    if fr[s].empty:
                        del fr[s]

                if freq:
                    fr = {k: fr[k].set_index(keys=['.ts'])
                                  .resample(freq).ffill()
                                  .reset_index()
                                  for k in fr}


                cols = []
                for s in fr.keys():
                    fr[s] = fr[s].set_index(keys=['.ts'])
                    sm = json.loads(s)
                    for col in fr[s].columns:
                        qualname = ":".join([sm['name'], col])
                        if qualname in cols:
                            raise Exception("overlapping column names: {}".format(col))
                        else:
                            cols.append(qualname)
                    fr[s].rename(columns={k: sm['name'] + ":" + k for k in fr[s].columns}, inplace=True)

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
                    spans.append({'cursor': t0['cursor'], 'start': t0.get('end', DTMAX), 'end': t1.get('start', DTMIN)})
            else:
                spans = []
            for sp in spans:
                start, end, cur = sp.get('start', DTMIN), sp.get('end', DTMAX), sp['cursor']
                data = self._slice(cur, start, end + horizon)
                fr = df(start, data)
                fr = {k: fr[k].set_index(keys=['.ts'])
                              .resample(freq).ffill()
                              .reset_index()
                              for k in fr}
                fts = max(fr[k]['.ts'][0] for k in fr)
                lts = min(fr[k]['.ts'][-1] for k in fr) + timedelta(seconds=1)


                cols = []
                for s in fr.keys():
                    fr[s] = fr[s].set_index(keys=['.ts'])
                    sm = json.loads(s)
                    for col in fr[s].columns:
                        qualname = ":".join([sm['name'], col])
                        if qualname in cols:
                            raise Exception("overlapping column names: {}".format(col))
                        else:
                            cols.append(qualname)
                    fr[s].rename(columns={k: sm['name'] + ":" + k for k in fr[s].columns}, inplace=True)

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

class ResultSpan(object):
    def __init__(self, cursor, start=None, end=None):
        self.cursor = cursor
        self.start = start
        self.end = end

    def __repr__(self):
        return "ResultSpan(start={}, end={}, cursor={})".format(self.start, self.end, self.cursor)

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
        """Return query results as a Pandas dataframe.

        Query results are returned as a dataframe with four index columns
        whose names are denoted with a `.`.

        .ts -- the datetime of the event
        .stream -- the stream the event came from
        .span -- the index of the span the event was found in
        .delta -- the timedelta from start of the span to this event

        The remaining columns of the dataframe are those specified by the
        user in the original query.

        Note: if multiple streams were queried, this function returns a
        a dictionary of dataframes where keys are the name of the stream and
        the frames are the values.

        Arguments:
            only -- Optional only return results from a provided stream name.

        Returns:
            data -- a Pandas dataframe with query results or a dictionary of
                    dataframes, one for each stream queried.
        """
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


def df(t0, data):
    dfs = {}
    for s in data['streams']:
        events = []
        for event in s['events']:
            evt = event['event']
            evt['.ts'] = cts(event['ts'])
            events.append(evt)
        dfs[json.dumps(s['stream'], sort_keys=True)] = json_normalize(events)
    return dfs

def build_url(host, stream, eid=None):
    """Build a url for the Sentenai API.

    Arguments:
        stream -- a stream object.
        eid -- an optional event id.

    Returns:
        url -- a URL for the Sentenai API endpoint to query a stream or event
    """
    if not isinstance(stream, Stream):
        raise TypeError("stream argument must be of type sentenai.Stream")

    def with_quoter(s):
        try:
            return quote(s)
        except:
            return quote(s.encode('utf-8', 'ignore'))

    url = [host, "streams", with_quoter(stream()['name'])]
    events = [] if eid is None else ["events", with_quoter(eid)]
    return "/".join(url + events)
