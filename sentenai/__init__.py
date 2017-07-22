import json

from sentenai.exceptions import *
from sentenai.utils import *
from sentenai.api import *
from sentenai.flare import *

try:
    from urllib.parse import quote
except:
    from urllib import quote

__all__ = ['FlareSyntaxError', 'LEFT', 'CENTER', 'RIGHT', 'Sentenai', 'span', 'any_of', 'all_of', 'V', 'delta', 'event', 'stream', 'select', 'ast', 'within_distance', 'inside_region']

#### Python 2 Compatibility Decorator


#### Constants

LEFT, CENTER, RIGHT = range(-1, 2)
DEFAULT = None


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

def within_distance(km, of):
    return InCircle(of, km)

def inside_region(poly):
    return InPolygon(poly)

#### Non-Flare Objects
