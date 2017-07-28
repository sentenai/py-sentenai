import json
import re
import requests

import pandas as pd

from pandas.io.json       import json_normalize
from datetime             import timedelta
from multiprocessing.pool import ThreadPool
from functools            import partial

from sentenai.exceptions import AuthenticationError, FlareSyntaxError, NotFound, SentenaiException, status_codes
from sentenai.utils import cts, dts, iso8601, LEFT, CENTER, RIGHT, DEFAULT, PY3
from sentenai.flare import EventPath, Stream, stream

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
            print(type(resp.json()))
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
        return FlareCursor(self, query, returning, limit)()


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

        return {'start': s, 'end': e, 'streams': streams.values()}

    def dataframe(self, only=None):
        if not self._data:
            self._events()

        data = self._data
        output = {}

        if len(data) < 1:
            if only is not None:
                return pd.DataFrame()
            else:
                return {}

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




class FlareCursor(object):

    def __init__(self, c, q, r, limit=None):
        self._client = c
        self._query = q
        self._returning = r
        self._limit = limit

    def __str__(self):
        return str(self._query)

    def __call__(self):
        return FlareResult(self._client, self._query, self._execute(self._query), self._returning)

    def _execute(self, query):
        headers = {'content-type': 'application/json', 'auth-key': self._client.auth_key}

        # POST a query
        if self._limit is None:
            url = '{host}/query'.format(host = self._client.host)
        else:
            url = '{host}/query?limit={limit}'.format(
                host = self._client.host,
                limit = self._limit)

        q = query()
        if self._returning:
            q['projections'] = {'explicit': []}
            for s,v in self._returning.items():
                if not isinstance(s, Stream):
                    raise FlareSyntaxError("returning dict top-level keys must be streams.")
                nd = {}
                if v is True:
                    q['projections']['explicit'].append({'stream': s(), 'projection': "default"})
                else:
                    q['projections']['explicit'].append({'stream': s(), 'projection': nd})

                    l = [(v, nd)]
                    while l:
                        old, new = l.pop(0)
                        for k,v in old.items():
                            if isinstance(v, EventPath):
                                z = v()
                                new[k] = [{'var': z['path'][1:]}]
                            elif isinstance(v, float):
                                new[k] = [{'lit': {'val': v, 'type': 'double'}}]
                            elif isinstance(v, int):
                                new[k] = [{'lit': {'val': v, 'type': 'int'}}]
                            elif isinstance(v, str):
                                new[k] = [{'lit': {'val': v, 'type': 'string'}}]
                            elif isinstance(v, bool):
                                new[k] = [{'lit': {'val': v, 'type': 'bool'}}]
                            elif isinstance(v, dict):
                                new[k] = {}
                                l.append((v,new[k]))
                            else:
                                raise FlareSyntaxError("%s: %s is unsupported." % (k, v.__class__))

        resp = requests.post(url, json = q, headers = headers )
        #print("finding spans took:", time.time() - a)

        # handle bad status codes
        if resp.status_code == 401:
            raise AuthenticationError("Invalid API Key")
        elif resp.status_code == 400:
            raise FlareSyntaxError
        elif resp.status_code >= 500:
            raise SentenaiException("Something went wrong.")
        elif resp.status_code != 200:
            raise Exception(resp.status_code)

        try:
            data = resp.json()
        except:
            raise
        else:
            return data


def is_nonempty_str(s):
    isNEstr = isinstance(s, str) and not (s == '')
    try:
        isNEuni = isinstance(s, unicode) and not (s == u'')
        return isNEstr or isNEuni
    except:
        return isNEstr


def build_url(host, stream, eid=None):
    if not isinstance(stream, Stream):
        raise TypeError("stream argument must be of type sentenai.Stream")

    if not is_nonempty_str(eid):
        raise TypeError("eid argument must be a non-empty string")

    def with_quoter(s):
        try:
            return quote(s)
        except:
            return quote(s.encode('utf-8', 'ignore'))

    url    = [host, "streams", with_quoter(stream()['name'])]
    events = [] if eid is None else ["events", with_quoter(eid)]
    return "/".join(url + events)


