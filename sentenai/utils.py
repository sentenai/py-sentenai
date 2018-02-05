import dateutil
import dateutil.tz
import sys
from datetime import datetime, timedelta, tzinfo

# Constants

PY3 = sys.version_info[0] == 3
LEFT, CENTER, RIGHT = range(-1, 2)
DEFAULT = None

if not PY3: import virtualtime

def py2str(cls):
    """Encode strings to utf-8 if the major version is not 3."""
    if not PY3:
        cls.__unicode__ = cls.__str__
        cls.__str__ = lambda self: self.__unicode__().encode('utf-8')
    return cls


class UTC(tzinfo):
    """A timezone class for UTC."""

    def dst(self, dt): return None

    def utcoffset(self, dt):
        """Generate a timedelta object with no offset."""
        return timedelta()


def iso8601(dt):
    """Convert a datetime object to an ISO8601 unix timestamp."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC())
    return dt.isoformat()


def cts(ts):
    """Convert a time string to a datetime object."""
    try:
        dt = dateutil.parser.parse(ts)
        if dt.tzinfo:
            return dt
        else:
            return dt.replace(tzinfo=dateutil.tz.tzutc())
    except:
        print("invalid time: " + ts)
        return ts


def dts(obj):
    """Convert a timestring to an ISO8601 unix timestamp."""
    if isinstance(obj, datetime):
        serial = iso8601(obj)
        return serial
    else:
        return obj


def divtime(l, r):
    numerator = l.days * 3600 * 24 + l.seconds
    divisor   = r.days * 3600 * 24 + r.seconds
    return int( numerator / divisor )


DTMIN = datetime.min.replace(tzinfo=dateutil.tz.tzutc())
DTMAX = datetime.max.replace(tzinfo=dateutil.tz.tzutc())
