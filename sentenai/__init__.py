import inspect, json, re, sys, time
import dateutil, requests, numpy, gevent, grequests
import pandas as pd
from pandas.io.json import json_normalize
from datetime import datetime, timedelta
try:
    from urllib.parse import quote
except:
    from urllib import quote

__all__ = ['FlareSyntaxError', 'LEFT', 'CENTER', 'RIGHT', 'Sentenai', 'span', 'any_of', 'all_of', 'V', 'delta', 'event', 'stream', 'select', 'ast']

#### Python 2 Compatibility Decorator

PY3 = sys.version_info[0] == 3

def py2str(cls):
    if not PY3:
        cls.__unicode__ = cls.__str__
        cls.__str__ = lambda self: self.__unicode__().encode('utf-8')
    return cls

#### Exceptions

class SentenaiException(Exception):
    pass

class FlareSyntaxError(SentenaiException):
    pass

class AuthenticationError(SentenaiException):
    pass

class NotFound(SentenaiException):
    pass

def status_codes(code):
    if code == 401:
        raise AuthenticationError("Invalid API key")
    elif code >= 500:
        raise SentenaiException("Something went wrong")
    elif code == 400:
        raise FlareSyntaxError()
    elif code == 404:
        raise NotFound()

    elif code >= 400:
        raise SentenaiException("something went wrong")

#### Constants

LEFT, CENTER, RIGHT = range(-1, 2)
DEFAULT = None


#### Flare Objects

class Flare(object):
    def __repr__(self):
        return str(self)

@py2str
class Switch(Flare):
    def __init__(self, *q, **kwargs):
        for c in q:
            if isinstance(c, Cond):
                if not isinstance(c.path, EventPath):
                    raise FlareSyntaxError('Use V. for paths within event()')
            else:
                raise FlareSyntaxError('Use V. for paths within event()')

        self._query = (tuple(q),)
        self._stream = None

    def __rshift__(self, nxt):
        s = Switch()
        s._query = (self._query + nxt._query)
        return s

    def bind(self, stream):
        sw = Switch()
        sw._query = self._query
        if not isinstance(stream, Stream):
            raise Exception("Can only bind switches to streams.")
        sw._stream = stream
        def bind2(self, *args, **kwargs):
            raise Exception("Cannot rebind switches.")
        sw.bind = bind2
        return sw

    def __call__(self):
        if len(self._query) <= 1:
            raise FlareSyntaxError("Switches must have two conditions")
        else:
            cds = []

            for s in self._query:
                if len(s) > 1:
                    cds.append({'type': '&&', 'args': [x() for x in s]})
                elif len(s) == 1:
                    cds.append(s[0]())
                else:
                    raise FlareSyntaxError("Switches must have non-empty conditions")

            return {'type': 'switch', 'conds': cds, 'stream': self._stream()}

    def __str__(self):
        if len(self._query) < 2:
            raise FlareSyntaxError("Switches must have two conditions")
        else:
            d = " -> ".join(" && ".join([str(x) if PY3 else str(x).decode('utf-8') for x in q]) for q in self._query)
            if self._stream:
                return "{}:({})".format(str(self._stream), d)
            else:
                return "(" + d + ")"

@py2str
class Select(Flare):
    """Select events from a span of time.

    Keyword arguments:
    start -- select events occuring at or after `datetime()`.
    end -- select events occuring before `datetime()`.
    """
    def __init__(self, **kwargs):
        self._after = kwargs.get("start")
        self._before = kwargs.get("end")
        self._query = []


    def span(self, *q, **kwargs):
        """A span of time where a set of conditions on one or more streams is continuously satisfied.

           Keyword arguments:
           min -- The minimum valid span duration `delta()`.
           max -- The maximum valid span duration `delta()`.
           exactly -- The exact valid span duration `delta()`.
           within -- The maximum distance in time between the end of the previous span and the start of this span.
           after -- The minimum distance in time between the end of the previous span and the start of this span.
        """
        for k in kwargs:
            if k not in ['min', 'max', 'exactly']:
                raise FlareSyntaxError('first span in a select supports only `min`, `max` and `exactly` duration arguments')
        if self._query:
            raise FlareSyntaxError("Use .then method")
        else:
            self._query.append(Span(*q, **kwargs))
        return self


    def then(self, *q, **kwargs):
        """A span of time following the previous span where a set of conditions are satisfied.

           Keyword arguments:
           min -- The minimum valid span duration `delta()`.
           max -- The maximum valid span duration `delta()`.
           exactly -- The exact valid span duration `delta()`.
           within -- The maximum distance in time between the end of the previous span and the start of this span.
           after -- The minimum distance in time between the end of the previous span and the start of this span.
        """
        if not self._query:
            raise FlareSyntaxError("Use .span method to start select")
        else:
            self._query.append(Span(*q, **kwargs))
        return self

    def __call__(self):
        if self._after and self._before:
            s = {'between': [self._after.isoformat(), self._before.isoformat()]}
        elif self._after:
            s = {'after': self._after.isoformat()}
        elif self._before:
            s = {'before': self._before.isoformat()}
        else:
            s = {}

        if len(self._query) == 0:
            raise FlareSyntaxError("Empty Select")
        elif len(self._query) == 1:
            s['select'] = self._query[0]()
        else:
            s['select'] = Serial(*self._query)()

        return s

    def __str__(self):
        if len(self._query) == 1:
            sep = " "
            q = str(self._query[0])

        else:
            sep = "\n    "
            q = str(Serial(*self._query))

        if not PY3: q = q.decode('utf-8')

        if not self._after and not self._before:
            s = "select" + sep + q
        elif not self._before:
            s = "select after {s:%Y-%m-%d %H:%M:%S%z}{sep}{q}".format(q=q, s=self._after, sep=sep)
        elif not self._after:
            s = "select before {s:%Y-%m-%d %H:%M:%S%z}{sep}{q}".format(q=q, e=self._before, sep=sep)
        else:
            s = "select from {s:%Y-%m-%d %H:%M:%S%z} until {e:%Y-%m-%d %H:%M:%S%z}{sep}{q}".format(q=q, s=self._after, e=self._before, sep=sep)
        return s


@py2str
class Cond(Flare):
    def __init__(self, path, op, val):
        self.path = path
        self.op = op
        self.val = val

    def __str__(self):
        if isinstance(self.val, str):
            val = '''"{}"'''.format(self.val)
        else:
            val = str(self.val)

        p = str(self.path) if PY3 else str(self.path).decode('utf-8')
        return "{path} {op} {val}".format(path=p, op=self.op, val=val)

    def __call__(self, stream=None):
        if isinstance(self.val, float):
            vt = 'double'
        elif isinstance(self.val, bool):
            vt = 'bool'
        elif isinstance(self.val, int):
            vt = 'double'
        else:
            vt = 'string'

        d = {'op': self.op, 'arg': {'type': vt, 'val': self.val}}
        if self.path.stream:
            d['type'] = 'span'
        if stream:
            d.update(self.path(stream))
        else:
            d.update(self.path())
        return d


    def __or__(self, q):
        return Or(self, q)


@py2str
class Stream(object):
    """A stream of events.

    Keywords arguments:
    name -- the name of a stream stored at https://api.senten.ai/streams/<name>.
    """
    def __init__(self, name, meta=None):
        self._name = quote(name.encode('utf-8'))
        if not meta:
            self._meta = {}
        else:
            self._meta = meta

    def __hash__(self):
        return hash(self._name)

    def _set(self, name):
        self._name = name

    def __repr__(self):
        return str(self)

    def __getitem__(self, sw):
        return sw.bind(self)

    def __str__(self):
        sub = False
        val = False
        for sf in inspect.stack():
            if not val:
                for k, v in sf[0].f_globals.items():
                    if v is self:
                        val = k
            if sf[4] and 'select' in sf[4][0]:
                sub = True
            elif sf[4] and 'Serial' in sf[4][0]:
                sub = True
        if sub and val: 
            return val
        else:
            return '(stream "{}")'.format(self._name)


    def __call__(self):
        return {'name': self._name}

    def __getattr__(self, name):
        return StreamPath((name,), self)

    def _(self, name):
        return StreamPath((name,), self)


@py2str
class EventPath(object):
    def __init__(self, namet=None):
        if namet == None:
            self.__attrlist = tuple()
        else:
            self.__attrlist = tuple(namet)

    def __getattr__(self, name):
        return EventPath(self.__attrlist + (name,))

    def _(self, name):
        return EventPath(self.__attrlist + (name,), self.__stream)

    def __eq__(self, val):
        """If used with an array, treat this as `in`.
        """
        if type(val) == list:
            return Cond(self, 'in', val)
        else:
            return Cond(self, '==', val)

    def __iter__(self):
        return iter(self.__attrlist)

    def __ne__(self, val):
        """If used with an array, treat this as `not in`.
        """
        return Cond(self, '!=', val)

    def __gt__(self, val):
        return Cond(self, '>', val)

    def __ge__(self, val):
        return Cond(self, '>=', val)

    def __le__(self, val):
        return Cond(self, '<=', val)

    def __lt__(self, val):
        return Cond(self, '<', val)

    def __repr__(self):
        return str(self)

    def __str__(self):
        return '{}'.format(".".join(self.__attrlist))

    def __call__(self):
        d = {'path': ('event',) + self.__attrlist}
        return d


@py2str
class StreamPath(object):
    """A stream's attribute path. Used to reference variables within events.

    Combine with operators like `==` and values to create condition objects.
    """
    def __init__(self, namet, stream=None):
        self.__stream = stream
        self.__attrlist = tuple(namet)


    def __getattr__(self, name):
        return StreamPath(self.__attrlist + (name,), self.__stream)

    def _(self, name):
        return StreamPath(self.__attrlist + (name,), self.__stream)

    def __eq__(self, val):
        """If used with an array, treat this as `in`.
        """
        if type(val) == list:
            return Cond(self, 'in', val)
        else:
            return Cond(self, '==', val)

    def __iter__(self):
        return iter(self.__attrlist)

    def __ne__(self, val):
        """If used with an array, treat this as `not in`.
        """
        return Cond(self, '!=', val)

    def __gt__(self, val):
        return Cond(self, '>', val)

    def __ge__(self, val):
        return Cond(self, '>=', val)

    def __le__(self, val):
        return Cond(self, '<=', val)

    def __lt__(self, val):
        return Cond(self, '<', val)

    def __repr__(self):
        return str(self)

    def __str__(self):
        attrs = [x if PY3 else x.decode('utf-8') for x in self.__attrlist]
        foo = ".".join(attrs)
        return '{stream}:{attrs}'.format(
                    stream = str(self.__stream),
                    attrs  = foo
                )

    def __call__(self):
        d = {'path': ('event',) + self.__attrlist, 'stream': self.__stream()}
        return d


@py2str
class Par(Flare):
    def __init__(self, f, q):
        self._f = f
        if len(q) < 1:
            raise FlareSyntaxError
        self.query = q

    def __str__(self):
        if len(self.query) < 1:
            raise FlareSyntaxError
        elif len(self.query) == 1:
            return str(self.query[0]) if PY3 else str(self.query[0]).decode('utf-8')
        else:
            ms = [str(x) if PY3 else str(x).decode('utf-8') for x in self.query]
            return self._f + " " + ",\n    ".join(ms)

    def __call__(self):
        if len(self.query) < 1:
            raise FlareSyntaxError
        elif len(self.query) == 1:
            return self.query[0]()
        else:
            return {'type': self._f, 'conds': [q() for q in self.query]}


@py2str
class Or(Flare):
    def __init__(self, *q):
        self.query = q

    def __call__(self):
        return {'expr': '||', 'args': [q() for q in self.query]}

    def __str__(self):
        qs = []
        for x in self.query:
            q = str(x) if PY3 else str(x).decode('utf-8')
            if isinstance(x, Span):
                if x._within is not None:
                    qs.append("(" + q + ")")
                else:
                    qs.append(q)
            else:
                qs.append(q)

        cs = " || ".join(qs)
        return cs

    def __or__(self, q):
        self.query.append(q)
        return self


@py2str
class Serial(Flare):
    def __init__(self, *q):
        self.query = []
        for x in q:
            if isinstance(x, Serial):
                self.query.extend(x.query)
            else:
                self.query.append(x)

    def then(self, *q, **kwargs):
        self.query.append(Span(*q, **kwargs))
        return self

    def __call__(self):
        return {'type': 'serial', 'conds': [q() for q in self.query]}

    def __str__(self):
        ss = [str(x) if PY3 else str(x).decode('utf-8') for x in self.query]
        return (";\n    ").join(ss)


@py2str
class Span(Flare):
    def __init__(self, *q, **kwargs):
        if len(q) < 1:
            raise FlareSyntaxError

        self.query = q
        self._within = kwargs.get('within')
        self._after = kwargs.get('after')
        self._min_width = kwargs.get('min')
        self._max_width = kwargs.get('max')
        self._width = kwargs.get('exactly')

    def __and__(self, q):
        return Span(self, q)

    def __or__(self, q):
        return Or(self, q)

    def __rshift__(self, q):
        return Serial(self, q)

    def __str__(self):
        qs = []
        for x in self.query:
            if isinstance(x, Span):
                if x._within is not None:
                    qs.append("(" + str(x) + ")")
                else:
                    qs.append(str(x))
            else:
                qs.append(str(x))

        cs = " && ".join([x if PY3 else x.decode('utf-8') for x in qs])

        if self._after:
            cs += " after {}".format(self._after)
        if self._within:
            cs += " within {}".format(self._within)
        if self._width:
            cs += " for exactly {}".format(self._width)
        elif self._min_width and not self._max_width:
            cs += " for at least {}".format(self._min_width)
        elif self._max_width and not self._min_width:
            cs += " for at most {}".format(self._max_width)
        elif self._max_width and self._min_width:
            cs += " for at least {} and at most {}".format(self._min_width, self._max_width)
        return cs

    def then(self, *q, **kwargs):
        return Serial(self, Span(*q, **kwargs))

    def __call__(self):
        d = {'for': {}}

        if self._within is not None:
            d['within'] = self._within()

        if self._after is not None:
            d['after'] = self._after()

        if self._min_width is not None:
            d['for']['at-least'] = self._min_width()

        if self._max_width is not None:
            d['for']['at-most'] = self._max_width()

        if self._width is not None:
            d['for'] = self._width()

        if not d['for']:
            del d['for']

        if len(self.query) == 1:
            if isinstance(self.query[0], Span):
                return merge(self, self.query[0])()
            else:
                d['type'] = 'span'
                d.update(self.query[0]())
        else:
            d['expr'] = '&&'
            d['args'] = [q() for q in self.query]
        return d


@py2str
class Delta(Flare):
    def __init__(self, seconds=0, minutes=0, hours=0, days=0, weeks=0, months=0, years=0):
        self.seconds=seconds
        self.minutes=minutes
        self.hours=hours
        self.days=days
        self.weeks=weeks
        self.months=months
        self.years=years

    def __compare__(self, other):
        if not isinstance(other, Delta):
            raise ValueError()
        return cmp(timedelta(**self()), timedelta(**other()))

    def __str__(self):
        fs = [self.seconds, self.minutes, self.hours, self.days, self.weeks, self.months, self.years]
        ls = "smhdwMy"
        return " ".join(["{}{}".format(int(a), x) for a, x in zip(fs,ls) if int(a) > 0])

    def __call__(self):
        r = {}
        if self.seconds > 0:
            r['seconds'] = self.seconds
        if self.minutes > 0:
            r['minutes'] = self.minutes
        if self.hours > 0:
            r['hours'] = self.hours
        if self.days > 0:
            r['days'] = self.days
        if self.weeks > 0:
            r['weeks'] = self.weeks
        if self.months > 0:
            r['months'] = self.months
        if self.years > 0:
            r['years'] = self.years

        return r or {'seconds': 0}


#### Utility functions


def cts(ts):
    try:
        return dateutil.parser.parse(ts)
    except:
        print("invalid time: "+ts)
        return ts

def dts(obj):
    if isinstance(obj, datetime):
        serial = obj.isoformat()
        return serial
    else:
        return obj

def merge(s1, s2):
    s3 = Span(*s2.query)
    if s1._within is None or s2._within is None:
        s3._within = s1._within or s2._within
    else:
        s3._within = min(s1._within, s2._within)
    s3._after  = max(s1._after, s2._after)
    s3._min_width = max(s1._min_width, s2._min_width)
    if s1._max_width is None or s2._max_width is None:
        s3._max_width = s1._max_width or s2._max_width
    else:
        s3._max_width = min(s1._max_width, s2._max_width)

    if s1._width and not s2._width:
        s3._width = s1._width
    elif s2._width and not s1._width:
        s3._width = s2._width
    elif s1._width != s2._width:
        s3._width = delta()
    else:
        s3._width = s2._width
    return s3



#### Convenience Functions

def stream(name, **kwargs):
    return Stream(name, **kwargs)

V = EventPath()

def event(*args, **kwargs):
    return Switch(*args, **kwargs)

def delta(seconds=0, minutes=0, hours=0, days=0, weeks=0, months=0, years=0):
    return Delta(**locals())

def ast(q):
    return json.dumps(q(), indent=4)

def select(start=None, end=None):
    """Select events from a span of time.

    Keyword arguments:
    start -- select events occuring at or after `datetime()`.
    end -- select events occuring before `datetime()`.
    """
    kwargs = {}
    if start: kwargs['start'] = start
    if end: kwargs['end'] = end
    return Select(**kwargs)


def span(*q, **kwargs):
    if len(q) == 1 and isinstance(q[0], Span):
        return q[0]
    else:
        return Span(*q, **kwargs)

def any_of(*q): return Par("any", q)

def all_of(*q): return Par("all", q)


#### Non-Flare Objects



class Sentenai(object):
    def __init__(self, auth_key=""):
        self.auth_key = auth_key
        self.host = "https://api.senten.ai"


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
        url = "/".join([self.host, "streams", stream()['name'], "events", eid])
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
            headers['timestamp'] = timestamp.isoformat()

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


    def streams(self):
        """Get list of available streams."""
        url = "/".join([self.host, "streams"])
        headers = {'auth-key': self.auth_key}
        resp = requests.get(url, headers=headers)
        status_codes(resp.status_code)
        try:
            return [stream(**v) for v in resp.json()]
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


    def newest(self, o):
        if isinstance(o, Stream):
            raise NotImplementedError
        else:
            raise SentenaiException("Must be called on stream")


    def oldest(self, o):
        if isinstance(o, Stream):
            raise NotImplementedError
        else:
            raise SentenaiException("Must be called on stream")


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
        spans = []
        for span in self._spans:
            s = cts(span['start'])
            e = cts(span['end'])
            c = span['cursor']
            spans.append(gevent.spawn(self._slice, s, e, c))
        a = time.time()
        gevent.joinall(spans)
        #print("finding events took:", time.time() - a)
        self._data = [span.value for span in spans]
        return self._data

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
                raise
                raise Exception("failed to get cursor")
            elif not resp.ok:
                retries += 1
                continue
            else:
                retries = 0
                c = resp.headers.get('cursor')
                data = resp.json()

                for sid, stream in data['streams'].items():
                    streams[sid] = {'stream': stream, 'events': []}

                for event in data['events']:
                    events = streams[event['stream']]['events']
                    ss = streams[event['stream']]['stream']
                    if self._returning is not None and ss in self._returning.keys():
                        evt = {}
                        for key, pth in self._returning[ss].items():
                            evt[key] = event['event']
                            for sg in pth:
                                try:
                                    evt[key] = evt[key][sg]
                                except:
                                    raise
                        event['event'] = evt
                    del event['stream']
                    events.append(event)

        return {'start': s, 'end': e, 'streams': streams.values()}

    def dataframe(self, only=None):
        if not self._data:
            self._events()

        data = self._data
        keys = {'.id', '.ts'}
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
                    if evt == None: continue
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

        return df.set_index(midx).drop(idx_names, axis=1)




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
        a = time.time()
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

