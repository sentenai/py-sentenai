import inspect, json, base64
from copy import copy
import numpy as np

from datetime import date, datetime, timedelta

from sentenai.exceptions import QuerySyntaxError
from sentenai.utils import iso8601, py2str, PY3

if not PY3: import virtualtime

try:
    from urllib.parse import quote
except:
    from urllib import quote


def delta(seconds=0, minutes=0, hours=0, days=0, weeks=0, months=0, years=0):
    """A convenience function for creating Delta objects.

    Deltas define specific lengths of time which can be used as part of
    conditions in a query.

    E.g. delta(hours=1, minutes=30) defines a length of time equaling an hour
    and thirty minutes which can be used to find sequences of events happening
    within an hour and thirty minutes of each other.

    Arugments:
        seconds -- number of seconds passed
        minutes -- number of minutes passed
        hours -- number of hours passed
        days -- number of days passed
        weeks -- number of weeks passed
        months -- number of months passed
        years -- number of years passed
    """
    return Delta(**locals())


class HistoriQL(object):
    """A HistoriQL query object."""

    def __repr__(self):
        """An unambiguous representation of the HistoriQL query."""
        return str(self)



class Projection(HistoriQL):
    def __add__(self, other):
        return ProjMath("+", self, other)

    def __radd__(self, other):
        return ProjMath("+", other, self)

    def __sub__(self, other):
        return ProjMath("-", self, other)

    def __rsub__(self, other):
        return ProjMath("-", other, self)

    def __mul__(self, other):
        return ProjMath("*", self, other)

    def __rmul__(self, other):
        return ProjMath("*", other, self)

    def __div__(self, other):
        return ProjMath("/", self, other)

    def __rdiv__(self, other):
        return ProjMath("/", other, self)

    def __or__(self, other):
        raise NotImplemented
        return ProjMath("/", other, self)

    def __ror__(self, other):
        raise NotImplemented
        return ProjMath("/", other, self)


class ProjMath(Projection):
    def __init__(self, op, p1, p2):
        self.lhs = p1
        self.rhs = p2
        self.op = op

    def __call__(self):

        def convert(p):
            if isinstance(p, float):
                return {'lit': {'val': p, 'type': 'double'}}
            elif isinstance(p, int):
                return {'lit': {'val': p, 'type': 'int'}}
            elif isinstance(p, EventPath):
                return {'var': ('event',) + p._attrlist}
            elif isinstance(p, ProjMath):
                return p()
            else:
                raise QuerySyntaxError("projection math with non-numeric types is unsupported.")
            return {'stream': self.stream(), 'projection': nd}

        return {'op': self.op, 'lhs': convert(self.lhs), 'rhs': convert(self.rhs)}


class Returning(object):
    def __init__(self, *streams, **kwargs):
        self.projs = []
        for s in streams:
            if isinstance(s, Stream):
                self.projs.append(Proj(s))
            elif isinstance(s, tuple):
                for x in s:
                    if isinstance(x, Stream):
                        self.projs.append(Proj(x))
                    elif isinstance(x, Proj):
                        self.projs.append(x)
            elif isinstance(s, Proj):
                self.projs.append(s)
            else:
                raise QuerySyntaxError("Invalid projection type in `returning`")
        self.default = kwargs.get('default', True)

    def __call__(self):
        return dict(projections={'explicit': [p() for p in self.projs], '...': self.default})



class Proj(object):
    def __init__(self, stream, proj=True):
        if not isinstance(stream, Stream):
            raise QuerySyntaxError("returning dict top-level keys must be streams.")
        self.stream = stream
        self.proj = proj

    def __call__(self):
        if self.proj is True:
            return {'stream': self.stream(), 'projection': "default"}
        elif self.proj is False:
            return {'stream': self.stream(), 'projection': False}
        else:
            nd = {}
            l = [(self.proj, nd)]
            while l:
                old, new = l.pop(0)
                for key, val in old.items():
                    if isinstance(val, EventPath):
                        z = ('event',) + val._attrlist
                        new[key] = [{'var': z}]
                    elif isinstance(val, ProjMath):
                        new[key] = [val()]
                    elif isinstance(val, float):
                        new[key] = [{'lit': {'val': val, 'type': 'double'}}]
                    elif isinstance(val, int):
                        new[key] = [{'lit': {'val': val, 'type': 'int'}}]
                    elif isinstance(val, str):
                        new[key] = [{'lit': {'val': val, 'type': 'string'}}]
                    elif isinstance(val, bool):
                        new[key] = [{'lit': {'val': val, 'type': 'bool'}}]
                    elif isinstance(val, dict):
                        new[key] = {}
                        l.append((val,new[key]))
                    else:
                        raise QuerySyntaxError("%s: %s is unsupported." % (key, val.__class__))
            return {'stream': self.stream(), 'projection': nd}



class InCircle(HistoriQL):
    """Used in conjunction with a Cond and shapely.geometry.Point."""

    def __init__(self, center, radius):
        """Initalize the object.

        Arguments:
            center -- the center of circle defined by a
                      shapely.geometry.Point object
            radius -- the radius of the circle in the units of the coordinate
                      system.
        """
        self.center = center
        self.radius = radius

    def __call__(self):
        """Generate the object in AST format."""
        return {
            'center': {
                'lat': self.center.y,
                'lon': self.center.x},
            'radius': self.radius
        }

    def __str__(self):
        """A string representation of the object."""
        return 'Circle{{lat:{}, lon:{}, radius:{}}}'.format(
            self.center.y, self.center.x, self.radius)


@py2str
class InPolygon(HistoriQL):
    """Used in conjuction with a Cond an shapely.geometry.Polygon."""

    def __init__(self, poly):
        """Initalize the object.

        Arguments:
            poly -- a shapely.geometry.Polygon object
        """
        self.poly = poly

    def __call__(self):
        """Generate the object in AST format."""
        vs = [{'lat': y, 'lon': x} for x, y in np.asarray(self.poly.exterior.coords)]  # NOQA
        return {"vertices": vs}

    def __str__(self):
        """A string representation of the object."""
        return "Polygon[{}]".format(", ".join(
            ['{{lat: {},  lon: {}}}'.format(x, y) for x, y in np.asarray(self.poly.exterior.coords)]))  # NOQA


@py2str
class Switch(HistoriQL):
    """A HistoriQL Switch condition.

    Switches are used to define transitions between events in sequences.
    You can define switches by applying the >> operator to events.

    E.g. `event1 >> event2` defines a pattern where event1 is followed by
    event2 in the stream.

    A single switch between two events is said to be "zero width" as it
    captures the timestamp of the transition, but does not capture either of
    the two events.

    Switches can be chained together to define patterns of arbitrary length.
    E.g. `event1 >> event2 >> event3`. In this case, a switch may capture
    events that happen between the first and last transition which will be
    included in query results.

    Because switchs can be created before they are bound to a stream, you will
    need to use the V object when defining events. For more information on
    using the V object see:
    http://docs.sentenai.com/#Mining_basic_time_series_patterns:_Heatwaves_in_Boston
    """

    def __init__(self, *q, **kwargs):
        """Initialize the switch.

        TODO: Define q and kwargs
        """
        for c in q:
            if isinstance(c, Cond):
                if not isinstance(c.path, EventPath):
                    raise QuerySyntaxError('Use V. for paths within event()')
            else:
                raise QuerySyntaxError('Use V. for paths within event()')

        self._query = (tuple(q),)
        self._stream = None

    def __rshift__(self, nxt):
        """Define the behavior of the right shift operator for switches.

        Arguments:
            nxt -- the event on the right hand side of the switch
        """
        s = Switch()
        s._query = (self._query + nxt._query)
        return s

    def _bind(self, stream):
        """Bind a stream to a switch statement.

        Arguments:
            stream -- a Sentenai stream object to bind
        """
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
        """Generate AST code from the switch."""
        if self._stream is None:
            raise QuerySyntaxError("Switch must be bound to stream")
        cds = []

        for s in self._query:
            if len(s) < 1:
                expr = {'expr': True}
            else:
                expr = s[-1]()
                for x in s[-2::-1]:
                    expr = {
                        'expr': '&&',
                        'args': [x(), expr]
                    }
                cds.append(expr)
        if len(cds) == 0:
            cds = [{'expr': True}, {'expr': True}]
        elif len(cds) == 1:
            cds.insert(0, {'expr': True})
        return {'type': 'switch', 'conds': cds, 'stream': self._stream()}

    def __str__(self):
        """Generate a string representation of the switch."""
        if len(self._query) == 0:
            d = "true -> true"
        elif len(self._query) == 1:
            d = "true -> {}".format(" && ".join([str(x) if PY3 else str(x).decode('utf-8') for x in self._query[0]]) or "true")
        else:
            d = " -> ".join(" && ".join([str(x) if PY3 else str(x).decode('utf-8') for x in q]) or "true" for q in self._query)  # NOQA
        if self._stream:
            return "{}:({})".format(str(self._stream), d)
        else:
            return "(" + d + ")"


@py2str
class Select(HistoriQL):
    """Select events from a span of time.

    Keyword arguments:
    start -- select events occuring at or after `datetime()`.
    end -- select events occuring before `datetime()`.
    """

    def __init__(self, *args):
        """Select data."""
        self._after = None
        self._before = None
        self._query = args

    def __getitem__(self, sl):
        x = Select(*self._query)
        if isinstance(sl, datetime):
            sl = slice(sl, sl + timedelta(1))
        x._after = sl.start
        x._before = sl.stop
        return x


    def __call__(self, *args):
        """Generate AST from the query object."""

        # if called with args, make new Select
        if args and not self._query:
            x = Select(*args)
            x._after = self._after
            x._before = self._before
            return Query(x)

        if self._after and self._before:
            s = {'between': [iso8601(self._after), iso8601(self._before)]}
        elif self._after:
            s = {'after': iso8601(self._after)}
        elif self._before:
            s = {'before': iso8601(self._before)}
        else:
            s = {}

        if len(self._query) == 0:
            s['select'] = {"expr": True}
        elif len(self._query) == 1:
            s['select'] = self._query[0]()
        else:
            s['select'] = Serial(*self._query)()

        return s


    def __str__(self):
        """Generate a string representation of the select."""
        if len(self._query) == 1:
            sep = " "
            q = str(self._query[0])

        else:
            sep = "\n    "
            q = str(Serial(*self._query))

        if not PY3:
            q = q.decode('utf-8')

        if not self._after and not self._before:
            s = "select" + sep + q
        elif not self._before:
            s = "select after {s:%Y-%m-%d %H:%M:%S%z}{sep}{q}".format(
                q=q, s=self._after, sep=sep)
        elif not self._after:
            s = "select before {e:%Y-%m-%d %H:%M:%S%z}{sep}{q}".format(
                q=q, e=self._before, sep=sep)
        else:
            s = ("select from {s:%Y-%m-%d %H:%M:%S%z} "
                 "until {e:%Y-%m-%d %H:%M:%S%z}{sep}{q}").format(
                q=q, s=self._after, e=self._before, sep=sep)
        return s


@py2str
class Cond(HistoriQL):
    """A HistoriQL condition.

    Conditions are used to search specific events or sets of
    events in a stream. You can define them explicitly using the class
    constructor or by applying operators to streams.

    >>> c = Cond(stream.attribute1, '>', 5)

    creates a condition that is satisfied
    when `attribute1` of stream is greater than 5. The same condition can be
    created using the short hand

    >>> c = stream.attribute1 > 5
    """

    def __init__(self, path, op, val):
        """Initialize the condition.

        Arguments:
            path -- the path to a specific attribute of a stream
            op -- the operator to be checked in the condition
            val -- the value to check the condition against.
        """
        self.path = path
        self.op = op
        self.val = val
        if isinstance(self.val, InPolygon) or isinstance(self.val, InCircle):
            if op not in ('==',):
                raise QuerySyntaxError(
                    "Only `==` operator can be used with regions")

    def __str__(self):
        """Generate a string representation of the condition."""
        if isinstance(self.val, str):
            val = '''"{}"'''.format(self.val)
        else:
            val = str(self.val)

        p = str(self.path) if PY3 else str(self.path).decode('utf-8')
        return "{path} {op} {val}".format(path=p, op=self.op, val=val)

    def __call__(self, stream=None):
        """Generate AST for the condition.

        Arguments:
            stream -- a stream to apply the condition to
        """
        val = self.val
        op = self.op
        tz = None
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
            val = "{}-{}-{}".format(
                self.val.year, self.val.month, self.val.day)
        elif isinstance(self.val, datetime):
            vt = "datetime"
            val = iso8601(self.val)
            try:
                tz = self.val.tzinfo.zone
            except:
                pass
        else:
            vt = 'string'

        d = {'op': op, 'arg': {'type': vt, 'val': val}}
        if isinstance(self.path, StreamPath):
            d['type'] = 'span'
        if stream:
            stream = copy(stream)
            stream.tz = tz
            d.update({'path': ('event',) + self.path._attrlist, 'stream': stream.name})
        elif isinstance(self.path, EventPath):
            d.update({'path': ('event',) + self.path._attrlist})
        else:
            d.update({'path': ('event',) + self.path._attrlist, 'stream': self.path._stream()})
        return d

    def __or__(self, q):
        """Define the `|` operator for conditions."""
        return Or(self, q)

    def __and__(self, q):
        """Define the `&` operator for conditions."""
        return And(self, q)

    def __rshift__(self, q):
        """Define the `>>` operator for spans.

        This operator is used chain spans together as a Serial object.

        Arguments:
            q -- a span to chain with this one.
        """
        return Serial(self, q)


class CondChain(HistoriQL):
    def __init__(self, op, lpath=None, lval=None, lcond=None, rpath=None, rval=None, rcond=None):
        self.op = op
        self.lpath = lpath
        self.rpath = rpath
        self.lcond = lcond
        self.rcond = rcond
        self.lval  = lval
        self.rval  = rval
        self.arr = [lpath, lcond, lval, op, rpath, rcond, rval]


    def reify(self):
        if self.lcond and self.rcond:
            if self.op is Switch:
                return self.op(self.lcond) >> self.op(self.rcond)
            else:
                return self.op(self.lcond, self.rcond)
        else:
            return self

    def __lt__(self, val):
        if not self.rpath:
            raise Exception("missing rpath")
        elif isinstance(val, CondChain):
            self.rval = val.lval
            self.rcond = Cond(self.rpath, '<', self.rval)
            val.lcond = self.reify()
            return val
        elif isinstance(self.rcond, CondChain):
            self.rcond = self.rcond < val
            return self.reify()
        else:
            self.rval = val
            self.rcond = Cond(self.rpath, '<', val)
            return self.reify()

    def __le__(self, val):
        if not self.rpath:
            raise Exception("missing rpath")
        elif isinstance(val, CondChain):
            self.rval = val.lval
            self.rcond = Cond(self.rpath, '<=', self.rval)
            val.lcond = self.reify()
            return val
        elif isinstance(self.rcond, CondChain):
            self.rcond = self.rcond <= val
            return self.reify()
        else:
            self.rval = val
            self.rcond = Cond(self.rpath, '<=', val)
            return self.reify()

    def __eq__(self, val):
        if not self.rpath:
            raise Exception("missing rpath")
        elif isinstance(val, CondChain):
            self.rval = val.lval
            self.rcond = Cond(self.rpath, '==', self.rval)
            val.lcond = self.reify()
            return val
        elif isinstance(self.rcond, CondChain):
            self.rcond = self.rcond == val
            return self.reify()
        else:
            self.rval = val
            self.rcond = Cond(self.rpath, '==', val)
            return self.reify()

    def __ne__(self, val):
        if not self.rpath:
            raise Exception("missing rpath")
        elif isinstance(val, CondChain):
            self.rval = val.lval
            self.rcond = Cond(self.rpath, '!=', self.rval)
            val.lcond = self.reify()
            return val
        elif isinstance(self.rcond, CondChain):
            self.rcond = self.rcond != val
            return self.reify()
        else:
            self.rval = val
            self.rcond = Cond(self.rpath, '!=', val)
            return self.reify()

    def __gt__(self, val):
        if not self.rpath:
            raise Exception("missing rpath")
        elif isinstance(val, CondChain):
            self.rval = val.lval
            self.rcond = Cond(self.rpath, '>', self.rval)
            val.lcond = self.reify()
            return val
        elif isinstance(self.rcond, CondChain):
            self.rcond = self.rcond > val
            return self.reify()
        else:
            self.rval = val
            self.rcond = Cond(self.rpath, '>', val)
            return self.reify()

    def __ge__(self, val):
        if not self.rpath:
            raise Exception("missing rpath")
        elif isinstance(val, CondChain):
            self.rval = val.lval
            self.rcond = Cond(self.rpath, '>=', self.rval)
            val.lcond = self.reify()
            return val
        elif isinstance(self.rcond, CondChain):
            self.rcond = self.rcond >= val
            return self.reify()
        else:
            self.rval = val
            self.rcond = Cond(self.rpath, '>=', val)
            return self.reify()

    def __str__(self):
        return "(({}) {} ({}))".format(
            (self.lcond if self.lcond else "{} {}".format(self.lpath, self.lval)),
            self.op,
            (self.rcond if self.rcond else "{} {}".format(self.rpath, self.rval))
        )

@py2str
class Stream(HistoriQL):
    """A stream of events.

    Stream objects reference streams of events stored in Sentenai. They are
    used when writing queries, access specific API end points, and manipulating
    result sets.
    """
    def __init__(self, name, meta, tz, *filters):
        """Initialize a stream object.

        Arguments:
            name    -- The name of a stream stored at
                       https://api.senten.ai/streams/<name>.
            meta    -- Meta data about the stream. TODO: This can be an arbitrary
                       object and does
                       not persist across Stream objects.
            info    -- TODO
            filters -- Conditions to be applied to the stream when filtering
                       events.
        """
        self._name = quote(name.encode('utf-8'))
        self._meta = meta
        self._filters = filters
        self.tz = tz

    def _serialized_filters(self):
        x = self().get('filter')
        if x:
            return {'filters': base64.urlsafe_b64encode(bytes(json.dumps(x), 'UTF-8'))}
        else:
            return {}

    def __pos__(self):
        return Proj(self, True)

    def __neg__(self):
        return Proj(self, False)

    def __mod__(self, pdict):
        return Proj(self, pdict)

    def __cmp__(self, other):
        raise QuerySyntaxError("cannot compare stream to value.")

    def __eq__(self, other):
        """Define the `==` operator for streams.

        Arguments:
            other -- the stream to compare with
        """
        try:
            return self._name == other._name \
                and self._serialized_filters() == other._serialized_filters()
        except AttributeError:
            return False

    def __hash__(self):
        """Generate a has of the stream.

        Currently streams with the same name have the same hash. Additional
        meta data and filters are not considered.
        """
        return hash(self._name)

    def _set(self, name):
        """Setter for a string's name attribute.

        Arguments:
            name -- the new name to set.
        """
        self._name = name

    def __repr__(self):
        """An unambiguous representation of a stream."""
        if not self._filters:
            return "Stream(name=\"{}\")".format(self._name)
        else:
            return "Stream(name=\"{}\", filters={})".format(
                self._name, self._filters)

    def __getitem__(self, key):
        """Get a StreamPath for a stream.

        Used primarily to escape segments of paths that would be invalid
        in the host language. For example, if a path segment contains `:`

        >>> s = stream("foo")
        >>> s.bar["..."].baz.bat

        Arguments:
            name -- The name of the variable to get.
        """
        return StreamPath((key,), self)

    def __str__(self):
        """A string representation of the stream object."""
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
                return '(stream "{}" with {})'.format(
                    self._name, self._filters)

    def __call__(self, sw=None):
        """Generate AST for the stream object including any filters.

        Arguments:
            sw -- TODO: define this.
        """
        if sw is None:
            b = {'name': self._name}
            if self.tz:
                b['timezone'] = self.tz
            if self._filters:
                s = self._filters
                expr = s[-1]()
                if 'type' in expr:
                    del expr['type']
                for x in s[-2::-1]:
                    y = x()
                    if 'type' in y:
                        del y['type']
                    expr = {
                        'expr': '&&',
                        'args': [y, expr]
                    }
                b['filter'] = expr
            return b
        else:
            try:
                return sw._bind(self)
            except AttributeError as e:
                raise TypeError(
                    "A stream should not be called with " + str(type(sw)), e)

    def __getattr__(self, name):
        """Get a SteamPath for a stream.

        StreamPaths are used to reference variables in events themselves.

        >>> s = stream("foo")
        >>> s.foo.bar.bat.baz

        Arguments:
            name -- The name of the variable to get
        """
        if not name.startswith('_') and name not in ['name', 'tz']:
            return StreamPath((name,), self)
        elif name == 'name':
            return self.__getattribute__("_name")
        else:
            return self.__getattribute__(name)



@py2str
class EventPath(Projection):
    """An event's attribute path.

    Used to reference variables within a single event. Combine with operators
    to create condition objects.
    """

    def __init__(self, namet=None):
        """Initialize the event path.

        Arguments:
            namet -- A list of variable names used to costruct a path. E.g.
                     ['foo', 'bar', 'baz'] becomes 'foo.bar.baz'
        """
        if not namet:
            self._attrlist = tuple()
        else:
            self._attrlist = tuple(namet)

    def __getattr__(self, name):
        """Get an EventPath for an event.

        Used to reference variables within an event.

        >>> evt.foo.bar.bat

        Arguments:
            name -- the name of the variable to get.
        """
        return EventPath(self._attrlist + (name,))

    def __getitem__(self, name):
        """Get an EventPath for an event.

        Used primarily to escape segments of paths that would be invalid
        in the host language. For example, if a path segment contains `:

        >>> evt.foo['...'].bar.bat

        Arguments:
            name -- the name of the variable to get.
        """
        if type(name) is int:
            return "asdfasdfasd"
        if hasattr(name, "isalnum"):
            return EventPath(self._attrlist + (name,))
        else:
            raise KeyError(name)

    def __eq__(self, val):
        """Create equality conditions for event variables.

        If used with an array, treat this as `in`.

        Arguments:
            val -- The value to compare the stream variable to.
        """
        if isinstance(val, CondChain):
            val.lcond = Cond(self, 'in' if type(val.lval) is list else '==', val.lval)
            val.lpath = self
            return val.reify()
        else:
            return Cond(self, 'in' if type(val) is list else '==', val)

    def __contains__(self, path):
        joiner = "$$$$%$$$$"
        return joiner.join(self._attrlist) in joiner.join(self._attrlist)


    def __iter__(self):
        return iter(self._attrlist)

    def __ne__(self, val):
        """Create inequality conditions for event variable.

        If used with an array, treat this as `not in`.

        Arguments:
            val -- The value to compare the stream variable to.
        """
        if isinstance(val, CondChain):
            val.lpath = self
            val.lcond = Cond(self, '!=', val.lval)
            return val.reify()
        else:
            return Cond(self, '!=', val)

    def __gt__(self, val):
        """Create greater than conditions for event variable.

        Arguments:
            val -- The value to compare the stream variable to.
        """
        if isinstance(val, CondChain):
            val.lpath = self
            val.lcond = Cond(self, '>', val.lval)
            return val.reify()
        else:
            return Cond(self, '>', val)

    def __ge__(self, val):
        """Create greater than or equal to conditions for event variable.

        Arguments:
            val -- The value to compare the stream variable to.
        """
        if isinstance(val, CondChain):
            val.lpath = self
            val.lcond = Cond(self, '>=', val.lval)
            return val.reify()
        else:
            return Cond(self, '>=', val)

    def __le__(self, val):
        """Create less than or equal to conditions for event variable.

        Arguments:
            val -- The value to compare the stream variable to.
        """
        if isinstance(val, CondChain):
            val.lpath = self
            val.lcond = Cond(self, '<=', val.lval)
            return val.reify()
        else:
            return Cond(self, '<=', val)

    def __lt__(self, val):
        """Create less than conditions for event variable.

        Arguments:
            val -- The value to compare the stream variable to.
        """
        if isinstance(val, CondChain):
            val.lpath = self
            val.lcond = Cond(self, '<', val.lval)
            return val.reify()
        else:
            return Cond(self, '<', val)

    def __repr__(self):
        """Generate an unambiguous representation of the EventPath."""
        return str(self)

    def __str__(self):
        """Generate a string representation of the EventPath."""
        return '{}'.format(".".join(self._attrlist))


    def __rrshift__(self, val):
        return CondChain(Switch, lval=val, rpath=self)

    def __rand__(self, other):
        return CondChain(And, lval=other, rpath=self)

    def __ror__(self, other):
        #return CondChain(Or, lval=other, rpath=self)
        raise HistoriQLSynxtaxError("Or not supported in event sequences.")


@py2str
class StreamPath(Projection):
    """A stream's attribute path. Used to reference variables within events.

    Combine with operators like `==` and values to create condition objects.
    """

    def __init__(self, namet, stream=None):
        """Initalize the StreamPath.

        Arguments:
            namet -- a list of names defining a path to an event variable
            stream -- a stream object to serve as the base path.
        """
        self._stream = stream
        self._attrlist = tuple(namet)

    def __iter__(self):
        return list(self._attrlist)

    def __getattr__(self, name):
        """Generate a new stream path by chaining two paths together.

        >>>s = stream('foo')
        >>>s.foo.bar

        Arguments:
            name -- a variable name to add to the path.
        """
        if not name.startswith('_'):
            return StreamPath(self._attrlist + (name,), self._stream)
        else:
            return self.__getattribute__(name)

    def __getitem__(self, name):
        """Generate a new stream path by chaining two paths together.

        This is a convinience function to escape invalid paths in the host
        language.

        >>> s = stream['foo']
        >>>s.foo['...'].bar

        Arguments:
            name -- a variable name to add to the path.
        """
        return StreamPath(self._attrlist + (name,), self._stream)

    def __contains__(self, path):
        joiner = "$$$$%$$$$"
        return joiner.join(self._attrlist) in joiner.join(self._attrlist)

    def __iter__(self):
        """Iterate through all levels of the StreamPath."""
        return iter(self._attrlist)

    def __eq__(self, val):
        """Create equality conditions for event variables.

        If used with an array, treat this as `in`.

        Arguments:
            val -- The value to compare the stream variable to.
        """
        if isinstance(val, CondChain):
            val.lcond = Cond(self, 'in' if type(val.lval) is list else '==', val.lval)
            val.arr = [self, '=='] + val.arr
            return val.reify()
        else:
            return Cond(self, 'in' if type(val) is list else '==', val)

    def __ne__(self, val):
        """Create inequality conditions for event variable.

        If used with an array, treat this as `not in`.

        Arguments:
            val -- The value to compare the stream variable to.
        """
        if isinstance(val, CondChain):
            val.lpath = self
            val.lcond = Cond(self, '!=', val.lval)
            return val.reify()
        else:
            return Cond(self, '!=', val)

    def __gt__(self, val):
        """Create greater than conditions for event variable.

        Arguments:
            val -- The value to compare the stream variable to.
        """
        if isinstance(val, CondChain):
            val.lpath = self
            val.lcond = Cond(self, '>', val.lval)
            return val.reify()
        else:
            return Cond(self, '>', val)

    def __ge__(self, val):
        """Create greater than or equal to conditions for event variable.

        Arguments:
            val -- The value to compare the stream variable to.
        """
        if isinstance(val, CondChain):
            val.lpath = self
            val.lcond = Cond(self, '>=', val.lval)
            return val.reify()
        else:
            return Cond(self, '>=', val)

    def __le__(self, val):
        """Create less than or equal to conditions for event variable.

        Arguments:
            val -- The value to compare the stream variable to.
        """
        if isinstance(val, CondChain):
            val.lpath = self
            val.lcond = Cond(self, '<=', val.lval)
            return val.reify()
        else:
            return Cond(self, '<=', val)

    def __lt__(self, val):
        """Create less than conditions for event variable.

        Arguments:
            val -- The value to compare the stream variable to.
        """
        if isinstance(val, CondChain):
            val.lpath = self
            val.lcond = Cond(self, '<', val.lval)
            return val.reify()
        else:
            return Cond(self, '<', val)

    def __repr__(self):
        """Generate an unambiguous representation of the EventPath."""
        return str(self)

    def __rand__(self, other):
        return CondChain(And, lval=other, rpath=self)

    def __ror__(self, other):
        return CondChain(Or, lval=other, rpath=self)


    def __repr__(self):
        """Generate an unambiguous representation of the StreamPath."""
        return ".".join(self._attrlist)

    def __str__(self):
        """Generate a string representation of the StreamPath."""
        attrs = [x if PY3 else x.decode('utf-8') for x in self._attrlist]
        foo = ".".join(["'{}'".format(attr) if '.' in attr else attr for attr in attrs])
        return '{stream}:{attrs}'.format(stream=str(self._stream), attrs=foo)

    def __call__(self, *args, **kwargs):
        if len(self._attrlist) == 1:
            return self._stream.__getattr__("_"+self._attrlist[0])(*args, **kwargs)
        elif self._attrlist[-1] == 'describe':
            return self._stream.__getattr__("_describe")(".".join(self._attrlist[:-1]), *args, **kwargs)
        elif self._attrlist[-1] == 'stats':
            return self._stream.__getattr__("_fstats")(".".join(self._attrlist[:-1]), *args, **kwargs)
        elif self._attrlist[-1] == 'unique':
            return self._stream.__getattr__("_unique")(".".join(self._attrlist[:-1]), *args, **kwargs)
        else:
            raise QuerySyntaxError("cannot call path")



@py2str
class Par(HistoriQL):
    """A HistoriQL Par Object.

    Par objects are used to define operators that act on sets of conditions.
    For example, we use a par object to define the ANY operator which returns
    True if any one of a set of conditions is true or the ALL operator which
    returns True if and only iff all conditions in a set are true.

    High level functions `all_of()` and `any_of()` are used to generate Par
    objects for these cases.
    """

    def __init__(self, f, q):
        """Initialize the Par object.

        TODO: rename these arguments to not be single letter and define them.

        Arguments:
            f -- a type of par. Will either be 'all' or 'any'
            q -- a query
        """
        self._f = f
        if len(q) < 1:
            raise QuerySyntaxError
        self.query = []
        for i in q:
            if isinstance(i, tuple):
                self.query.append(Serial(*i))
            else:
                self.query.append(i)

    def __str__(self):
        """Generate a string representation of the par."""
        if len(self.query) < 1:
            raise QuerySyntaxError
        elif len(self.query) == 1:
            return str(self.query[0]) if PY3 else str(self.query[0]).decode('utf-8')  # NOQA
        else:
            ms = [str(x) if PY3 else str(x).decode('utf-8') for x in self.query]  # NOQA
            return self._f + " " + ",\n    ".join(ms)

    def __call__(self):
        """Generate an AST representation of the Par."""
        if len(self.query) < 1:
            raise QuerySyntaxError
        elif len(self.query) == 1:
            return self.query[0]()
        else:
            return {'type': self._f, 'conds': [q() for q in self.query]}


@py2str
class Or(HistoriQL):
    """A HistoriQL Or object.

    TODO: Check my understanding here.
    The Or object is used to compare two spans. If the conditions of either
    span are met, events are returned.
    """

    def __init__(self, *q):
        """Initialize the Or.

        Arguments:
            q -- queries to join with an or.
        """
        self._within = None
        self._after = None
        self._width = None
        self.query = []
        for i in q:
            if isinstance(i, tuple):
                self.query.append(Serial(*i))
            else:
                self.query.append(i)


    def __call__(self):
        """Generate an AST representation of the Or."""
        if len(self.query) == 0:
            raise HistoriQLSynxtaxError('Not enough arguments in Or')
        elif len(self.query) == 1:
            return self.query[0]()
        else:
            d = {'expr': '||', 'args': [self.query[0](), self.query[-1]()]}
            for q in self.query[-2:0:-1]:
                d['args'][1] = {
                    'expr': '||',
                    'args': [q(), d['args'][1]]
                }

            if self._within is not None:
                d['within'] = self._within()

            if self._after is not None:
                d['after'] = self._after()

            if self._width is not None:
                d.update(self._width())

            return d


    def __str__(self):
        """Generate a string representation of the Or."""
        qs = []
        for x in self.query:
            q = str(x) if PY3 else str(x).decode('utf-8')
            if isinstance(x, And):
                if x._within is not None:
                    qs.append("(" + q + ")")
                else:
                    qs.append(q)
            else:
                qs.append(q)

        cs = " || ".join(qs)
        return cs


    def __or__(self, q):
        """Define the behavior of `|` operator.

        Arguments:
            q -- a query to or together with existing queries.
        """
        self.query += (q,)
        return self



@py2str
class Serial(HistoriQL):
    """A Serial object.

    Serial objects are used to define queries looking for chains of events or
    spans that occur in sequence over time. For example, looking for a
    temperature spike followed by a temperature drop in weather data.

    Serial objects provide a way to query for complex patterns in events.
    """

    def __init__(self, *q):
        self.query = []
        self._within = None
        self._after = None
        self._width = None
        for i in q:
            if isinstance(i, tuple):
                self.query.append(Serial(*i))
            else:
                self.query.append(i)

    def __call__(self):
        """Generate an AST representation of the Serial."""
        gs = group_spans(self.query)
        if len(gs) == 0:
            d = {'expr': True}
        elif len(gs) == 1:
            d = gs[0]()
        else:
            d = {'type': 'serial', 'conds': [q() for q in gs]}

        if self._within is not None:
            d['within'] = self._within()

        if self._after is not None:
            d['after'] = self._after()

        if self._width is not None:
            d.update(self._width())

        return d

    def __str__(self):
        """Generate a string representation of the Serial."""
        ss = [str(x) if PY3 else str(x).decode('utf-8') for x in self.query]
        return (";\n    ").join(ss)


@py2str
class And(HistoriQL):
    """A And of time where events continuously satisfy a set of conditions.

    A span is defined by looking for events that continuously meet a set of
    conditions. For example, a simple span of time in weather data may be
    a continues number of days where the temperature is below freezing.
    Conditions can be chained together to find more complicated patterns.
    """

    def __init__(self, *q):
        """Initialize the And.

        Arguments:
            q -- a set of conditions
            min -- The minimum valid span duration `delta()`.
            max -- The maximum valid span duration `delta()`.
            exactly -- The exact valid span duration `delta()`.
            within -- The maximum distance in time between the end of the
                      previous span and the start of this span.
            after -- The minimum distance in time between the end of the
                     previous span and the start of this span.
        """
        if len(q) < 1:
            raise QuerySyntaxError

        self._within = None
        self._after = None
        self._width = None

        self.query = []
        for i in q:
            if isinstance(i, tuple):
                self.query.append(Serial(*i))
            else:
                self.query.append(i)

    def __and__(self, q):
        """Define the `and` operator for spans.

        Arguments:
            q -- a span to `and` with this one.
        """
        return And(self, q)

    def __or__(self, q):
        """Define the `or` operator for spans.

        Arguments:
            q -- a span to `or` with this one.
        """
        return Or(self, q)

    def __rshift__(self, q):
        """Define the `>>` operator for spans.

        This operator is used chain spans together as a Serial object.

        Arguments:
            q -- a span to chain with this one.
        """
        return Serial(self, q)

    def __str__(self):
        """Generate a string representation of the And."""
        qs = []
        for x in self.query:
            if isinstance(x, And):
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
            cs += " {}".format(self._width)
        return cs


    def __call__(self):
        """Generate an AST representation of the span."""
        d = {}

        if self._within is not None:
            d['within'] = self._within()

        if self._after is not None:
            d['after'] = self._after()

        if self._width is not None:
            d.update(self._width())

        if len(self.query) == 1:
            if isinstance(self.query[0], And):
                return merge(self, self.query[0])()
            elif isinstance(self.query[0], Or):
                d.update(self.query[0]())
            else:
                d['type'] = 'span'
                d.update(self.query[0]())
        else:
            d['expr'] = '&&'
            d['args'] = [self.query[0](), self.query[-1]()]
            for q in self.query[-2:0:-1]:
                d['args'][1] = {
                    'expr': '&&',
                    'args': [q(), d['args'][1]]
                }


        return d


@py2str
class Delta(HistoriQL):
    """A Delta object.

    Delta objects represent durations of time
    """

    def __init__(self, seconds=0, minutes=0, hours=0,
                 days=0, weeks=0, months=0, years=0):
        """Initialize the Delta.

        The instantiated delta object is a summation of all arguments.
        E.g. `Delta(minutes=1, seconds=30) defines a duration of 1 minute
        and 30 seconds

        Arguments:
            seconds -- the number of seconds in the delta.
            minutes -- the number of minutes in the delta.
            hours -- the number of hours in the delta.
            days -- the number of days in the delta.
            weeks -- the number of weeks in the delta.
            months -- the number of months in the delta.
            years -- the number of years in the delta.
        """
        self.seconds = seconds
        self.minutes = minutes
        self.hours = hours
        self.days = days
        self.weeks = weeks
        self.months = months
        self.years = years
        self.timedelta = timedelta(
            days=days + 7 * 4 * months + 365 * years,
            seconds=seconds,
            microseconds=0,
            milliseconds=0,
            minutes=minutes,
            hours=hours,
            weeks=weeks
        )

    def __compare__(self, other):
        """A comparator of deltas.

        Arguments:
            other -- another durationt to compare to.
        """
        if not isinstance(other, Delta):
            raise ValueError()
        return cmp(timedelta(**self()), timedelta(**other()))

    def __str__(self):
        """Generate a string representation of the delta."""
        fs = [self.seconds, self.minutes, self.hours, self.days,
              self.weeks, self.months, self.years]
        ls = "smhdwMy"
        return " ".join(
            ["{}{}".format(int(a), x) for a, x in zip(fs, ls) if int(a) > 0]
        )

    def __call__(self):
        """Generate an AST representation of the Delta."""
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
        """Define the `==` operator for deltas.

        Arguments:
            val -- the other delta to compare with
        """
        if val is None:
            return False
        typecheck(Delta, 'val', val)
        return self.timedelta == val.timedelta

    def __gt__(self, val):
        """Define the `>` operator for deltas.

        Arguments:
            val -- the other delta to compare with
        """
        typecheck(Delta, 'val', val)
        return self.timedelta > val.timedelta

    def __ge__(self, val):
        """Define the `>=` operator for deltas.

        Arguments:
            val -- the other delta to compare with
        """
        typecheck(Delta, 'val', val)
        return self.timedelta >= val.timedelta

    def __le__(self, val):
        """Define the `<=` operator for deltas.

        Arguments:
            val -- the other delta to compare with
        """
        typecheck(Delta, 'val', val)
        return self.timedelta <= val.timedelta

    def __lt__(self, val):
        """Define the `<` operator for deltas.

        Arguments:
            val -- the other delta to compare with
        """
        typecheck(Delta, 'val', val)
        return self.timedelta < val.timedelta

class Modifier(Delta): pass
class Within(Modifier): pass
class After(Modifier): pass

class Lasting(Modifier):

    def __call__(self):
        """Generate an AST representation of the Delta."""
        if (self._exactly, self._min, self._max) == (None, None, None):
            raise QuerySyntaxError('empty `Lasting`')
        elif self._exactly != None:
            if (self._min, self._max) != (None, None):
                raise QuerySyntaxError('Cannot have both exact and ranged `Lasting`')
            else:
                return {'for': self._exactly()}
        else:
            x = {'for': {}}
            if self._min != None:
                x['for']['at-least'] = self._min()
            if self._max != None:
                x['for']['at-most'] = self._max()
            return x

    @staticmethod
    def exactly(**kwargs):
        return LastingExactly(**kwargs)

    @staticmethod
    def min(**kwargs):
        return LastingMin(**kwargs)

    @staticmethod
    def max(**kwargs):
        return LastingMax(**kwargs)

class LastingExactly(Lasting):

    def __str__(self):
        """Generate a string representation of the delta."""
        return str(self._exactly)

    def __init__(self, **kwargs):
        self._exactly = Delta(**kwargs)
        self._min = None
        self._max = None

    def max(self, **kwargs):
        raise QuerySyntaxError("Cannot apply `max` to exact duration.")

    def min(self, **kwargs):
        raise QuerySyntaxError("Cannot apply `min` to exact duration.")


class LastingMax(Lasting):

    def __str__(self):
        """Generate a string representation of the delta."""
        return str(self._max)

    def __init__(self, **kwargs):
        self._exactly = None
        self._min = None
        self._max = Delta(**kwargs)

    def max(self, **kwargs):
        if kwargs == None: return self
        raise QuerySyntaxError("Cannot apply `max` twice.")

    def min(self, **kwargs):
        if kwargs == None: return self
        return LastingRange(Delta(**kwargs), self._max)


class LastingMin(Lasting):

    def __str__(self):
        """Generate a string representation of the delta."""
        return str(self._min)

    def __init__(self, **kwargs):
        self._exactly = None
        self._min = Delta(**kwargs)
        self._max = None

    def min(self, **kwargs):
        if kwargs == None: return self
        raise QuerySyntaxError("Cannot apply `max` twice.")

    def max(self, **kwargs):
        if kwargs == None: return self
        return LastingRange(self._min, Delta(**kwargs))

class LastingRange(Lasting):

    def __str__(self):
        """Generate a string representation of the delta."""
        return "{} to {}".format(self._min, self._max)

    def __init__(self, mindt, maxdt):
        self._exactly = None
        self._min = mindt
        self._max = maxdt

    def min(self, **kwargs):
        if kwargs == None: return self
        raise QuerySyntaxError("Cannot apply `max` twice.")

    def min(self, **kwargs):
        if kwargs == None: return self
        raise QuerySyntaxError("Cannot apply `min` twice.")


def merge(s1, s2):
    """Merge two spans.

    When two spans are merged, the resulting span retains the minimum `within`
    duration, the maximum `after` duration, the largest `min` duration, and the
    smallest `max` duration. The final width of the resulting span is the zero
    if the two widths are not equal.

    Arugments:
        s1 -- the first span
        s2 -- the second span
    """
    typecheck(And, 'left side of merge', s1)
    typecheck(And, 'right side of merge', s2)
    s3 = And(*s2.query)

    def go(op, attr):
        a1 = s1.__getattribute__(attr)
        a2 = s2.__getattribute__(attr)
        if a1 is None or a2 is None:
            return a1 or a2
        else:
            return op(a1, a2)

    def merge_width(width1, width2):
        minw = maxw = exw = None
        if width1._min != None and width2._min != None:
            minw = max(width1._min, width2._min)
        else:
            minw = width1._min or width2._min

        if width1._max != None and width2._max != None:
            maxw = min(width1._max, width2._max)
        else:
            maxw = width1._max or width2._max

        if width1._exactly != None and width2._exactly != None:
            if width1._exactly != width2._exactly:
                raise QuerySyntaxError("Cannot decide between two exact durations.")
            else:
                exw = width1._exactly
        else:
            exw = width1._exactly or width2._exactly

        if minw > maxw:
            raise QuerySyntaxError("Min duration larger than max duration")
        elif exw and minw and maxw:
            if maxw >= exw >= minw:
                minw = None
                maxw = None
            else:
                raise QuerySyntaxError("Cannot have both exact range and duration")
        elif exw and minw:
            if exw > minw:
                minw = None
            else:
                raise QuerySyntaxError("Conflicting durations in query.")
        elif exw and maxw:
            if exw < maxw:
                maxw = None
            else:
                raise QuerySyntaxError("Conflicting durations in query.")

        x = Lasting()
        x._min = minw
        x._max = maxw
        x._exactly = exw
        return x




        return delta() if width1 != width2 else width1

    s3._within = go(min, '_within')
    s3._after = go(max, '_after')
    s3._width = go(merge_width, '_width')

    return s3


def validate_kwargs(valid_set, input_kwargs):
    """Validate kework arguments.

    Throw an error explaining to a user if they failed to pass in the
    correct keyword arguments.

    Arguments:
        valid_set :: (set|frozenset)[str] -- a set of acceptable keyword
                                             arguments
        input_kwargs :: dict[str, Any]    -- expected to be the **kwargs of
                                             a function
    """
    if len(set(input_kwargs.keys()) - valid_set) > 0:
        raise TypeError(
            "input kwargs should only be one of: " + str(valid_set)
        )


def typecheck(types, k, v):
    """Throw an error explaining to a user if the value is incorrect.

    Arguments:
        types :: type | list[type]  -- a type or types to check
        k     :: str                -- keyword of the argument
        v     :: Any                -- value of the argument
    """
    if (isinstance(types, list) and not all(map(lambda typ: isinstance(v, typ), types))):  # NOQA
        raise ValueError(
            "argument {} must be one of the following types: {}".format(
                k, str(types)))
    elif not isinstance(v, types):
        raise ValueError(
            "argument {} must be of type {}".format(k, str(types)))


def typecheck_kwargs(valid_types_dict, input_kwargs):
    """Check the type of kwargs.

    Throw a human-readable error explaining to a user if any input kwargs
    are incorrect.

    Arguments:
        valid_types_dict :: dict[str, (type|list[type])]  -- a book of keywords and a type or types to check  # NOQA
        input_kwargs     :: dict[str, Any]                -- expected to be the **kwargs of a function  # NOQA
    """
    if len(input_kwargs) == 0:
        pass
    else:
        validate_kwargs(set(valid_types_dict.keys()), input_kwargs)
        for k, v in input_kwargs.items():
            typecheck(valid_types_dict[k], k, v)



class Query(HistoriQL):
    def __init__(self, *statements):
        self.statements = statements

    def __call__(self):
        """Generate an Abstract Syntax Tree for a given query"""
        q, r = None, None
        for s in self.statements:
            if isinstance(s, Select):
                if q is None:
                    q = s()
                else:
                    raise QuerySyntaxError("Only one `select` statement may be present in a query")
            elif isinstance(s, And):
                if q is None:
                    z = Select()
                    z._query = [s]
                    q = z()
                else:
                    raise QuerySyntaxError("Only one `select` statement may be present in a query")
            elif isinstance(s, Returning):
                if r is None:
                    r = s()
                else:
                    raise QuerySyntaxError("Only one `returning` statement may be present in a query")
            else:
                raise QuerySyntaxError("Statement must be either `returning` or `select`")

        if q is None:
            q = Select()()
        if r is None:
            r = {}

        q.update(r)
        return q

    def __str__(self):
        return "\n".join(map(str, self.statements))

# Needed for testing
def ast_dict(*statements):
    return Query(Select(*statements))()

def ast(*statements):
    """Print the query as an Abstract Syntax Tree JSON string"""
    return json.dumps(ast_dict(*statements), indent=4)




def group_spans(qparts):
    spans = []
    for q in qparts:
        if isinstance(q, Within):
            spans[-1]._within = q
        elif isinstance(q, After):
            spans[-1]._after = q
        elif isinstance(q, Lasting):
            spans[-1]._width = q
        elif isinstance(q, And):
            spans.append(q)
        elif isinstance(q, Or):
            spans.append(q)
        elif isinstance(q, Cond):
            spans.append(And(q))
        elif isinstance(q, Switch):
            spans.append(q)
        elif isinstance(q, Par):
            spans.append(q)
        elif isinstance(q, Serial):
            spans.append(q)
        elif isinstance(q, tuple):
            spans.append(Serial(*q))
        else:
            raise QuerySyntaxError
    return spans
