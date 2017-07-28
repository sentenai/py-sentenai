import inspect
import numpy as np
import sys

from datetime import date, datetime, timedelta

from sentenai.exceptions import FlareSyntaxError
from sentenai.utils import iso8601, py2str, PY3

try:
    from urllib.parse import quote
except:
    from urllib import quote



def delta(seconds=0, minutes=0, hours=0, days=0, weeks=0, months=0, years=0):
    return Delta(**locals())


class Flare(object):
    def __repr__(self):
        return str(self)


class InCircle(Flare):
    """ used in conjunction with a Cond and shapely.geometry.Point """
    def __init__(self, center, radius):
        self.center = center
        self.radius = radius

    def __call__(self):
        return {
            'center': {
                'lat': self.center.y,
                'lon': self.center.x },
            'radius': self.radius
        }

    def __str__(self):
        return 'Circle{{lat:{}, lon:{}, radius:{}}}'.format(self.center.y, self.center.x, self.radius)

@py2str
class InPolygon(Flare):
    def __init__(self, poly):
        self.poly = poly

    def __call__(self):
        vs = [{'lat': y, 'lon': x} for x, y in np.asarray(self.poly.exterior.coords)]
        return {"vertices": vs}

    def __str__(self):
        return "Polygon[{}]".format(", ".join(['{{lat: {},  lon: {}}}'.format(x, y) for x, y in np.asarray(self.poly.exterior.coords)]))




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

    def _bind(self, stream):
        sw = Switch()
        sw._query = self._query
        if not isinstance(stream, Stream):
            raise Exception("Can only bind switches to streams.")
        sw._stream = stream
        def bind2(self, *args, **kwargs):
            raise Exception("Cannot rebind switches.")
        sw._bind = bind2
        return sw

    def __call__(self):
        if len(self._query) <= 1:
            raise FlareSyntaxError("Switches must contain at least two `event()`'s")
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
            if "after" not in kwargs and "within" not in kwargs:
                kwargs["within"] = delta(seconds=0)
            self._query.append(Span(*q, **kwargs))
        return self

    def __call__(self):
        if self._after and self._before:
            s = {'between': [iso8601(self._after), iso8601(self._before)]}
        elif self._after:
            s = {'after': iso8601(self._after)}
        elif self._before:
            s = {'before': iso8601(self._before)}
        else:
            s = {}

        if len(self._query) == 0:
            s['select'] = {"expr": "true"}
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
            s = "select before {e:%Y-%m-%d %H:%M:%S%z}{sep}{q}".format(q=q, e=self._before, sep=sep)
        else:
            s = "select from {s:%Y-%m-%d %H:%M:%S%z} until {e:%Y-%m-%d %H:%M:%S%z}{sep}{q}".format(q=q, s=self._after, e=self._before, sep=sep)
        return s


@py2str
class Cond(Flare):
    def __init__(self, path, op, val):
        self.path = path
        self.op = op
        self.val = val
        if isinstance(self.val, InPolygon) or isinstance(self.val, InCircle):
            if op not in ('==',):
                raise FlareSyntaxError("Only `==` operator can be used with regions")

    def __str__(self):
        if isinstance(self.val, str):
            val = '''"{}"'''.format(self.val)
        else:
            val = str(self.val)

        p = str(self.path) if PY3 else str(self.path).decode('utf-8')
        return "{path} {op} {val}".format(path=p, op=self.op, val=val)

    def __call__(self, stream=None):
        val = self.val
        op = self.op
        if isinstance(self.val, float):
            vt = 'double'
        elif isinstance(self.val, bool):
            vt = 'bool'
        elif isinstance(self.val, int):
            vt = 'double'
        elif isinstance(self.val, InPolygon):
            vt = "polygon"
            op = "in"
            val = self.val()
        elif isinstance(self.val, InCircle):
            vt = "circle"
            op = "in"
            val = self.val()
        elif isinstance(self.val, date):
            vt = "date"
            val = "{}-{}-{}".format(self.val.year, self.val.month, self.val.day)
        elif isinstance(self.val, datetime):
            vt = "datetime"
            val = iso8601(self.val)
        else:
            vt = 'string'

        d = {'op': op, 'arg': {'type': vt, 'val': val}}
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
    def __init__(self, name, meta, *filters):
        self._name = quote(name.encode('utf-8'))
        self._meta = meta
        self._filters = filters

    def __eq__(self, other):
        try:
            return self._name == other._name
        except AttributeError:
            return False


    def __hash__(self):
        return hash(self._name)

    def _set(self, name):
        self._name = name

    def __repr__(self):
        if not self._filters:
            return "Stream(name=\"{}\")".format(self._name, self._filters)
        else:
            return "Stream(name=\"{}\", filters={})".format(self._name, self._filters)

    def __getitem__(self, key):
        if key == "name":
            return self._name
        elif key == "meta":
            return self._meta
        else:
            raise KeyError

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
            if not self._filters:
                return '(stream "{}")'.format(self._name)
            else:
                return '(stream "{}" with {})'.format(self._name, self._filters)


    def __call__(self, sw=None):
        if sw is None:
            b = {'name': self._name}
            if self._filters:
                if len(self._filters) > 1:
                    b['filter'] = {'type': '&&', 'args': [x() for x in self._filters]}
                elif len(self._filters) == 1:
                    b['filter'] = self._filters[0]()
            return b
        else:
            try:
                return sw._bind(self)
            except AttributeError as e:
                raise TypeError("A stream should not be called with " + str(type(sw)), e)

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
        return EventPath(self.__attrlist + (name,))

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
        if "after" not in kwargs and "within" not in kwargs:
            kwargs["within"] = delta(seconds=0)
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
        if "after" not in kwargs and "within" not in kwargs:
            kwargs["within"] = delta(seconds=0)
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
        self.timedelta = timedelta(days=days + 7*4*months + 365*years, seconds=seconds, microseconds=0, milliseconds=0, minutes=minutes, hours=hours, weeks=weeks)

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

    def __eq__(self, val):
        typecheck(Delta, 'val', val)
        return self.timedelta == val.timedelta

    def __gt__(self, val):
        typecheck(Delta, 'val', val)
        return self.timedelta > val.timedelta

    def __ge__(self, val):
        typecheck(Delta, 'val', val)
        return self.timedelta >= val.timedelta

    def __le__(self, val):
        typecheck(Delta, 'val', val)
        return self.timedelta <= val.timedelta

    def __lt__(self, val):
        typecheck(Delta, 'val', val)
        return self.timedelta < val.timedelta


def stream(name, *args, **kwargs):
    """Define a stream, possibly with a list of filter arguments."""
    return Stream(name, kwargs.get('meta', {}), *args)

def merge(s1, s2):
    typecheck(Span, 'left side of merge', s1)
    typecheck(Span, 'right side of merge', s2)
    s3 = Span(*s2.query)

    def go(op, attr):
        a1 = s1.__getattribute__(attr)
        a2 = s2.__getattribute__(attr)
        if a1 is None or a2 is None:
            return a1 or a2
        else:
            return op(a1, a2)

    def delta_or_first(width1, width2):
        return delta() if width1 != width2 else width1

    s3._within    = go(min, '_within')
    s3._after     = go(max, '_after')
    s3._min_width = go(max, '_min_width')
    s3._max_width = go(min, '_max_width')
    s3._width     = go(delta_or_first, '_width')

    return s3


def validate_kwargs(valid_set, input_kwargs):
    """
    Throw an error explaining to a user if they failed to pass in the correct keyword arguments.

    valid_set    :: (set|frozenset)[str]  -- a set of acceptable keyword arguments
    input_kwargs :: dict[str, Any]        -- expected to be the **kwargs of a function
    """
    if len(set(input_kwargs.keys()) - valid_set) > 0:
        raise TypeError("input kwargs should only be one of: " + str(valid_set))


def typecheck(types, k, v):
    """
    Throw an error explaining to a user if the value is incorrect

    types :: type | list[type]  -- a type or types to check
    k     :: str                -- keyword of the argument
    v     :: Any                -- value of the argument
    """
    if isinstance(types, list) and not all(map(lambda typ: isinstance(v, typ), types)):
        raise ValueError("argument {} must be one of the following types: {}".format(k, str(types)))
    elif not isinstance(v, types):
        raise ValueError("argument {} must be of type {}".format(k, str(types)))


def typecheck_kwargs(valid_types_dict, input_kwargs):
    """
    Throw a human-readable error explaining to a user if any input kwargs are incorrect.

    valid_types_dict :: dict[str, (type|list[type])]  -- a book of keywords and a type or types to check
    input_kwargs     :: dict[str, Any]                -- expected to be the **kwargs of a function
    """
    if len(input_kwargs) == 0:
        pass
    else:
        validate_kwargs(set(valid_types_dict.keys()), input_kwargs)
        for k, v in input_kwargs.items():
            typecheck(valid_types_dict[k], k, v)


