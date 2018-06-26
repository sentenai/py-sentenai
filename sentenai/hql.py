__all__ = ["Within", "After", "Lasting", "During", "And", "Or", 'V']

from sentenai.historiQL import Within, After, Lasting, And, Or, EventPath, Par, InCircle, InPolygon

from sentenai.utils import PY3


# Python 2 Compatibility Decorator
if not PY3: import virtualtime


# Convenience Functions


def event(*args, **kwargs):
    return Switch(*args, **kwargs)



def within_distance(km, of):
    """Return all events within a given distance (in km) from a point."""
    return InCircle(of, km)


def inside_region(poly):
    """Return all events within a polygon."""
    return InPolygon(poly)

class Any(object):
    def __new__(self, *q):
        return Par("any", q)


class All(object):
    def __new__(self, *q):
        return Par("all", q)


class During(object):
    def __new__(self, *q):
        return Par("during", q)


V = EventPath()
