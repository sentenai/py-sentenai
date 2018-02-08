import inspect, json
from copy import copy
import numpy as np

from datetime import date, datetime, timedelta

from sentenai.exceptions import FlareSyntaxError
from sentenai.utils import iso8601, py2str, PY3

if not PY3: import virtualtime

try:
    from urllib.parse import quote
except:
    from urllib import quote


def delta(seconds=0, minutes=0, hours=0, days=0, weeks=0, months=0, years=0):
    """A convience function for creating Delta objects.

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


class Flare(object):
    """A Flare query object."""

    def __repr__(self):
        """An unambiguous representation of the Flare query."""
        return str(self)



class Projection(Flare):
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
                return {'var': p()['path']}
            elif isinstance(p, ProjMath):
                return p()
            else:
                raise FlareSyntaxError("projection math with non-numeric types is unsupported.")
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
                raise FlareSyntaxError("Invalid projection type in `returning`")
        self.default = kwargs.get('default', True)

    def __call__(self):
        return dict(projections={'explicit': [p() for p in self.projs], '...': self.default})



class Proj(object):
    def __init__(self, stream, proj=True):
        if not isinstance(stream, Stream):
            raise FlareSyntaxError("returning dict top-level keys must be streams.")
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
                        z = val()
                        new[key] = [{'var': z['path']}]
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
                        raise FlareSyntaxError("%s: %s is unsupported." % (key, val.__class__))
            return {'stream': self.stream(), 'projection': nd}



class InCircle(Flare):
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
class InPolygon(Flare):
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
class Switch(Flare):
    """A Flare Switch condition.

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
                    raise FlareSyntaxError('Use V. for paths within event()')
            else:
                raise FlareSyntaxError('Use V. for paths within event()')

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
        if len(self._query) <= 1:
            raise FlareSyntaxError(
                "Switches must contain at least two `event()`'s")
        else:
            cds = []

            for s in self._query:
                if len(s) < 1:
                    raise FlareSyntaxError("Switches must have non-empty conditions")
                expr = s[-1]()
                for x in s[-2::-1]:
                    expr = {
                        'expr': '&&',
                        'args': [x(), expr]
                    }
                cds.append(expr)

            return {'type': 'switch', 'conds': cds, 'stream': self._stream()}

    def __str__(self):
        """Generate a string representation of the switch."""
        if len(self._query) < 2:
            raise FlareSyntaxError("Switches must have two conditions")
        else:
            d = " -> ".join(" && ".join([str(x) if PY3 else str(x).decode('utf-8') for x in q]) for q in self._query)  # NOQA
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
        """Initialize the select.

        TODO: Define what kwargs can be.
        Arguments:
            start -- the minimum timestamp an event can have
            end -- the maximum timestamp an event can have
            kwargs -- additional parameters
        """
        self._after = kwargs.get("start")
        self._before = kwargs.get("end")
        self._query = []

    def span(self, *q, **kwargs):
        """A span of time where a set of conditions is continuously satisfied.

        Conditions can be defined across one or more streams.

        Keyword arguments:
            min -- The minimum valid span duration `delta()`.
            max -- The maximum valid span duration `delta()`.
            exactly -- The exact valid span duration `delta()`.
            within -- The maximum distance in time between the end of the
                      previous span and the start of this span.
            after -- The minimum distance in time between the end of the
                     previous span and the start of this span.
        """
        for k in kwargs:
            if k not in ['min', 'max', 'exactly']:
                raise FlareSyntaxError(
                    'first span in a select supports only '
                    '`min`, `max` and `exactly` duration arguments')
        if self._query:
            raise FlareSyntaxError("Use .then method")
        else:
            self._query.append(Span(*q, **kwargs))
        return self

    def then(self, *q, **kwargs):
        """A span of time following the previous span satisfying new conditions.

        Conditions can be defined across one or more streams and must also
        be satisfied continuously.

        Keyword arguments:
           min -- The minimum valid span duration `delta()`.
           max -- The maximum valid span duration `delta()`.
           exactly -- The exact valid span duration `delta()`.
           within -- The maximum distance in time between the end of
                     the previous span and the start of this span.
           after -- The minimum distance in time between the end of
                    the previous span and the start of this span.
        """
        if not self._query:
            raise FlareSyntaxError("Use .span method to start select")
        else:
            if "after" not in kwargs and "within" not in kwargs:
                kwargs["within"] = delta(seconds=0)
            self._query.append(Span(*q, **kwargs))
        return self

    def __call__(self):
        """Generate AST from the query object."""
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
class Cond(Flare):
    """A Flare condition.

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
                raise FlareSyntaxError(
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
        if self.path.stream:
            d['type'] = 'span'
        if stream:
            stream = copy(stream)
            stream.tz = tz
            d.update(self.path(stream))
        else:
            d.update(self.path())
        return d

    def __or__(self, q):
        """Define the `|` operator for conditions."""
        return Or(self, q)


@py2str
class Stream(object):
    """A stream of events.

    Stream objects reference streams of events stored in Sentenai. They are
    used when writing queries, access specific API end points, and manipulating
    result sets.
    """
    def __init__(self, name, meta, info, tz, *filters):
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
        self._info = info
        self._filters = filters
        self.tz = tz

    def __pos__(self):
        return Proj(self, True)

    def __neg__(self):
        return Proj(self, False)

    def __mod__(self, pdict):
        return Proj(self, pdict)

    def __eq__(self, other):
        """Define the `==` operator for streams.

        Arguments:
            other -- the stream to compare with
        """
        try:
            return self._name == other._name
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
        """Get attibutes of the stream object itself.

        >>> stream[key]

        This method does not get attributes of stream events. For that see
        `this.__getattr__()`.

        Arguments:
            key -- the name of the attribut to get.
                   Can be either 'name' or 'meta'
        """
        if key == "name":
            return self._name
        elif key == "meta":
            return self._meta
        elif key == "info":
            return self._info
        else:
            raise KeyError

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
        return StreamPath((name,), self)

    def _(self, name):
        """Get a StreamPath for a stream.

        Used primarily to escape segments of paths that would be invalid
        in the host language. For example, if a path segment contains `:`

        >>> s = stream("foo")
        >>> s.bar._("...").baz.bat

        Arguments:
            name -- The name of the variable to get.
        """
        return StreamPath((name,), self)


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
            self.__attrlist = tuple()
        else:
            self.__attrlist = tuple(namet)

    def __getattr__(self, name):
        """Get an EventPath for an event.

        Used to reference variables within an event.

        >>> evt.foo.bar.bat

        Arguments:
            name -- the name of the variable to get.
        """
        return EventPath(self.__attrlist + (name,))

    def _(self, name):
        """Get an EventPath for an event.

        Used primarily to escape segments of paths that would be invalid
        in the host language. For example, if a path segment contains `:

        >>> evt.foo._('...').bar.bat

        Arguments:
            name -- the name of the variable to get.
        """
        return EventPath(self.__attrlist + (name,))

    def __eq__(self, val):
        """Create equality conditions for event variables.

        If used with an array, treat this as `in`.

        Arguments:
            val -- The value to compare the stream variable to.
        """
        if type(val) == list:
            return Cond(self, 'in', val)
        else:
            return Cond(self, '==', val)

    def __iter__(self):
        """An iterator for event paths."""
        return iter(self.__attrlist)

    def __ne__(self, val):
        """Create inequality conditions for event variable.

        If used with an array, treat this as `not in`.

        Arguments:
            val -- The value to compare the stream variable to.
        """
        return Cond(self, '!=', val)

    def __gt__(self, val):
        """Create greater than conditions for event variable.

        Arguments:
            val -- The value to compare the stream variable to.
        """
        return Cond(self, '>', val)

    def __ge__(self, val):
        """Create greater than or equal to conditions for event variable.

        Arguments:
            val -- The value to compare the stream variable to.
        """
        return Cond(self, '>=', val)

    def __le__(self, val):
        """Create less than or equal to conditions for event variable.

        Arguments:
            val -- The value to compare the stream variable to.
        """
        return Cond(self, '<=', val)

    def __lt__(self, val):
        """Create less than conditions for event variable.

        Arguments:
            val -- The value to compare the stream variable to.
        """
        return Cond(self, '<', val)

    def __repr__(self):
        """Generate an unambiguous representation of the EventPath."""
        return str(self)

    def __str__(self):
        """Generate a string representation of the EventPath."""
        return '{}'.format(".".join(self.__attrlist))

    def __call__(self):
        """Generate an AST representation of the EventPath."""
        d = {'path': ('event',) + self.__attrlist}
        return d


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
        self.__stream = stream
        self.__attrlist = tuple(namet)

    def __getattr__(self, name):
        """Generate a new stream path by chaining two paths together.

        >>>s = stream('foo')
        >>>s.foo.bar

        Arguments:
            name -- a variable name to add to the path.
        """
        return StreamPath(self.__attrlist + (name,), self.__stream)

    def _(self, name):
        """Generate a new stream path by chaining two paths together.

        This is a convinience function to escape invalid paths in the host
        language.

        >>> s = stream('foo')
        >>>s.foo._('...').bar

        Arguments:
            name -- a variable name to add to the path.
        """
        return StreamPath(self.__attrlist + (name,), self.__stream)

    def __eq__(self, val):
        """Create an equality condition for stream event variables.

        If used with an array, treat this as `in`.

        Arguments:
            val -- the value to compare stream attributes to.
        """
        if type(val) == list:
            return Cond(self, 'in', val)
        else:
            return Cond(self, '==', val)

    def __iter__(self):
        """Iterate through all levels of the StreamPath."""
        return iter(self.__attrlist)

    def __ne__(self, val):
        """Create a not equal condition for stream event variables.

        If used with an array, treat this as `not in`.

        Arguments:
            val -- the value to compare stream attributes to.
        """
        return Cond(self, '!=', val)

    def __gt__(self, val):
        """Create a greater than condition for stream event variables.

        Arguments:
            val -- the value to compare stream attributes to.
        """
        return Cond(self, '>', val)

    def __ge__(self, val):
        """Create a greater than or equal condition for stream event variables.

        Arguments:
            val -- the value to compare stream attributes to.
        """
        return Cond(self, '>=', val)

    def __le__(self, val):
        """Create a less than or equal condition for stream event variables.

        Arguments:
            val -- the value to compare stream attributes to.
        """
        return Cond(self, '<=', val)

    def __lt__(self, val):
        """Create a less than condition for stream event variables.

        Arguments:
            val -- the value to compare stream attributes to.
        """
        return Cond(self, '<', val)

    def __repr__(self):
        """Generate an unambiguous representation of the StreamPath."""
        return str(self)

    def __str__(self):
        """Generate a string representation of the StreamPath."""
        attrs = [x if PY3 else x.decode('utf-8') for x in self.__attrlist]
        foo = ".".join(attrs)
        return '{stream}:{attrs}'.format(stream=str(self.__stream), attrs=foo)

    def __call__(self):
        """Generate an AST representation of the StreamPath."""
        d = {'path': ('event',) + self.__attrlist, 'stream': self.__stream()}
        return d


@py2str
class Par(Flare):
    """A Flare Par Object.

    Par objects are used to define operators that act on sets of conditions.
    For example, we use a par object to define the ANY operator which returns
    True if any one of a set of conditions is true or the ALL operator which
    returns True if and only iff all conditionsin a set are true.

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
            raise FlareSyntaxError
        self.query = q

    def __str__(self):
        """Generate a string representation of the par."""
        if len(self.query) < 1:
            raise FlareSyntaxError
        elif len(self.query) == 1:
            return str(self.query[0]) if PY3 else str(self.query[0]).decode('utf-8')  # NOQA
        else:
            ms = [str(x) if PY3 else str(x).decode('utf-8') for x in self.query]  # NOQA
            return self._f + " " + ",\n    ".join(ms)

    def __call__(self):
        """Generate an AST representation of the Par."""
        if len(self.query) < 1:
            raise FlareSyntaxError
        elif len(self.query) == 1:
            return self.query[0]()
        else:
            return {'type': self._f, 'conds': [q() for q in self.query]}


@py2str
class Or(Flare):
    """A Flare Or object.

    TODO: Check my understanding here.
    The Or object is used to compare two spans. If the conditions of either
    span are met, events are returned.
    """

    def __init__(self, *q):
        """Initialize the Or.

        Arguments:
            q -- queries to join with an or.
        """
        self.query = q

    def __call__(self):
        """Generate an AST representation of the Or."""
        if len(self.query) == 0:
            raise FlareSynxtaxError('Not enough arguments in Or')
        elif len(self.query) == 1:
            return self.query[0]()
        else:
            d = {'expr': '||', 'args': [self.query[0](), self.query[-1]()]}
            for q in self.query[-2:0:-1]:
                d['args'][1] = {
                    'expr': '||',
                    'args': [q(), d['args'][1]]
                }
            return d


    def __str__(self):
        """Generate a string representation of the Or."""
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
        """Define the behavior of `|` operator.

        Arguments:
            q -- a query to or together with existing queries.
        """
        self.query.append(q)
        return self


@py2str
class Serial(Flare):
    """A Serial object.

    Serial objects are used to define queries looking for chains of events or
    spans that occur in sequence over time. For example, looking for a
    temperature spike followed by a temperature drop in weather data.

    Serial objects provide a way to query for complex patterns in events.
    """

    def __init__(self, *q):
        """Initialize the Serial.

        TODO: Define q in this case
        Arguments:
            q --

        """
        self.query = []
        for x in q:
            if isinstance(x, Serial):
                self.query.extend(x.query)
            else:
                self.query.append(x)

    def then(self, *q, **kwargs):
        """A span of time following the previous span satisfying new conditions.

        Arguments:
            q -- conditions to query for
            min -- The minimum valid span duration `delta()`.
            max -- The maximum valid span duration `delta()`.
            exactly -- The exact valid span duration `delta()`.
            within -- The maximum distance in time between the end of the
                      previous span and the start of this span.
            after -- The minimum distance in time between the end of the
                     previous span and the start of this span.
        """
        if "after" not in kwargs and "within" not in kwargs:
            kwargs["within"] = delta(seconds=0)
        self.query.append(Span(*q, **kwargs))
        return self

    def __call__(self):
        """Generate an AST representation of the Serial."""
        return {'type': 'serial', 'conds': [q() for q in self.query]}

    def __str__(self):
        """Generate a string representation of the Serial."""
        ss = [str(x) if PY3 else str(x).decode('utf-8') for x in self.query]
        return (";\n    ").join(ss)


@py2str
class Span(Flare):
    """A Span of time where events continuously satisfy a set of conditions.

    A span is defined by looking for events that continuously meet a set of
    conditions. For example, a simple span of time in weather data may be
    a continues number of days where the temperature is below freezing.
    Conditions can be chained together to find more complicated patterns.
    """

    def __init__(self, *q, **kwargs):
        """Initialize the Span.

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
            raise FlareSyntaxError

        self.query = q
        self._within = kwargs.get('within')
        self._after = kwargs.get('after')
        self._min_width = kwargs.get('min')
        self._max_width = kwargs.get('max')
        self._width = kwargs.get('exactly')

    def __and__(self, q):
        """Define the `and` operator for spans.

        Arguments:
            q -- a span to `and` with this one.
        """
        return Span(self, q)

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
        """Generate a string representation of the Span."""
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
            cs += " for at least {} and at most {}".format(
                self._min_width, self._max_width)
        return cs

    def then(self, *q, **kwargs):
        """A span of time following the previous span satisfying new conditions.

        Arguments:
            q -- conditions to query for
            min -- The minimum valid span duration `delta()`.
            max -- The maximum valid span duration `delta()`.
            exactly -- The exact valid span duration `delta()`.
            within -- The maximum distance in time between the end of the
                      previous span and the start of this span.
            after -- The minimum distance in time between the end of the
                     previous span and the start of this span.
        """
        if "after" not in kwargs and "within" not in kwargs:
            kwargs["within"] = delta(seconds=0)
        return Serial(self, Span(*q, **kwargs))

    def __call__(self):
        """Generate an AST representation of the span."""
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
class Delta(Flare):
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


def stream(name, *args, **kwargs):
    """Define a stream, possibly with a list of filter arguments."""
    tz = kwargs.get('tz')
    return Stream(name, kwargs.get('meta', {}), kwargs.get('info', {}), tz, *args)


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

    s3._within = go(min, '_within')
    s3._after = go(max, '_after')
    s3._min_width = go(max, '_min_width')
    s3._max_width = go(min, '_max_width')
    s3._width = go(delta_or_first, '_width')

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



class Query(Flare):
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
                    raise FlareSyntaxError("Only one `select` statement may be present in a query")
            elif isinstance(s, Span):
                if q is None:
                    z = Select()
                    z._query = [s]
                    q = z()
                else:
                    raise FlareSyntaxError("Only one `select` statement may be present in a query")
            elif isinstance(s, Returning):
                if r is None:
                    r = s()
                else:
                    raise FlareSyntaxError("Only one `returning` statement may be present in a query")
            else:
                raise FlareSyntaxError("Statement must be either `returning` or `select`")

        if q is None:
            q = Select()()
        if r is None:
            r = {}

        q.update(r)
        return q

# Needed for testing
def ast_dict(*statements):
    return Query(*statements)()

def ast(*statements):
    """Print the query as an Abstract Syntax Tree JSON string"""
    return json.dumps(ast_dict(*statements), indent=4)




