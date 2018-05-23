import json

from sentenai.historiQL import *
from sentenai.api import Sentenai
from sentenai.utils import LEFT, RIGHT, CENTER, PY3


__all__ = [
    'QuerySyntaxError',
    'LEFT', 'CENTER', 'RIGHT',
    'Sentenai',
    'And', 'Or', 'Any', 'All', 'During',
    'V', 'delta', 'event',
    'ast', 'within_distance', 'inside_region',
    'Within', 'After', 'Lasting',
    'Select', 'Returning'
]

# Python 2 Compatibility Decorator
if not PY3: import virtualtime


# Convenience Functions
V = EventPath()

Select = Select()


def event(*args, **kwargs):
    return Switch(*args, **kwargs)


class Any(object):
    def __new__(self, *q):
        return Par("any", q)


class All(object):
    def __new__(self, *q):
        return Par("any", q)


class During(object):
    def __new__(self, *q):
        return Par("during", q)


def within_distance(km, of):
    """Return all events within a given distance (in km) from a point."""
    return InCircle(of, km)


def inside_region(poly):
    """Return all events within a polygon."""
    return InPolygon(poly)
