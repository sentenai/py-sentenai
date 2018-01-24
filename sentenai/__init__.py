import json

from sentenai.flare import (
    delta, stream, EventPath, FlareSyntaxError, InCircle, InPolygon, Par,
    Select, Span, Switch, merge, ast, Returning
)
from sentenai.api import Sentenai
from sentenai.utils import LEFT, RIGHT, CENTER, PY3


__all__ = [
    'FlareSyntaxError', 'LEFT', 'CENTER', 'RIGHT', 'Sentenai', 'span',
    'any_of', 'all_of', 'V', 'delta', 'event', 'returning', 'stream', 'select',
    'ast', 'within_distance', 'inside_region', 'merge'
]

# Python 2 Compatibility Decorator
if not PY3: import virtualtime

# Flare Objects


# Convenience Functions
V = EventPath()


def event(*args, **kwargs):
    return Switch(*args, **kwargs)


def select(start=None, end=None):
    """Select events from a span of time.

    Arguments:
    start -- select events occuring at or after `datetime()`.
    end -- select events occuring before `datetime()`.
    """
    kwargs = {}
    if start:
        kwargs['start'] = start
    if end:
        kwargs['end'] = end
    return Select(**kwargs)

def _span(*q, **kwargs):
    return Select().span(*q, **kwargs)

select.span = _span

def returning(*args, **kwargs):
    """
    Define projections for query results where each key is a string and each
    value is either a literal (int, bool, float, str) or an EventPath `V.foo`
    that corresponds to an existing path within the stream's events e.g.
        >>> boston = stream('weather')
        >>> returning(boston % {
                'high': V.temperatureMax,
                'low': V.temperatureMin,
                'ccc': {
                    'foo': 534.2,
                    'bar': "hello, world!"
                }
            })
    """
    return Returning(*args, **kwargs)


def span(*q, **kwargs):
    """Define a span of time."""
    if len(q) == 1 and isinstance(q[0], Span):
        return q[0]
    else:
        return Span(*q, **kwargs)


def any_of(*q):
    """Return events that match any specified conditions."""
    return Par("any", q)


def all_of(*q):
    """Return events that match all specified conditions."""
    return Par("all", q)


def within_distance(km, of):
    """Return all events within a given distance (in km) from a point."""
    return InCircle(of, km)


def inside_region(poly):
    """Return all events within a polygon."""
    return InPolygon(poly)
