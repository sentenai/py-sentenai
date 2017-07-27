import json

from sentenai.flare import delta, stream, EventPath, FlareSyntaxError, InCircle, InPolygon, Par, Select, Span, Switch, merge
from sentenai.api import Sentenai
from sentenai.utils import LEFT, RIGHT, CENTER



__all__ = ['FlareSyntaxError', 'LEFT', 'CENTER', 'RIGHT', 'Sentenai', 'span', 'any_of', 'all_of', 'V', 'delta', 'event', 'stream', 'select', 'ast', 'within_distance', 'inside_region', 'merge']

#### Python 2 Compatibility Decorator


#### Flare Objects


#### Convenience Functions
V = EventPath()


def event(*args, **kwargs):
    return Switch(*args, **kwargs)


def ast(q):
    return json.dumps(q(), indent=4)


def select(start=None, end=None):
    """Select events from a span of time.

    Keyword arguments:
    start -- select events occuring at or after `datetime()`.
    end -- select events occuring before `datetime()`.
    """
    kwargs = {}
    if start:
        kwargs['start'] = start
    if end:
        kwargs['end'] = end
    return Select(**kwargs)


def span(*q, **kwargs):
    if len(q) == 1 and isinstance(q[0], Span):
        return q[0]
    else:
        return Span(*q, **kwargs)


def any_of(*q):
    return Par("any", q)


def all_of(*q):
    return Par("all", q)


def within_distance(km, of):
    return InCircle(of, km)


def inside_region(poly):
    return InPolygon(poly)
