import dateutil
import dateutil.tz
import sys, base64, math
import simplejson as json
from datetime import datetime, timedelta, tzinfo, time

# Constants

PY3 = sys.version_info[0] == 3
LEFT, CENTER, RIGHT = range(-1, 2)
DEFAULT = None

if not PY3: import virtualtime


def base64json(x):
    if PY3:
        return base64.urlsafe_b64encode(bytes(json.dumps(x), 'UTF-8'))
    else:
        return base64.urlsafe_b64encode(json.dumps(x))

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
        return ts


def dts(obj):
    """Convert a datetime to an ISO8601 unix timestamp."""
    if isinstance(obj, datetime):
        serial = iso8601(obj)
        return serial
    if isinstance(obj, time):
        return time.isoformat()
    else:
        return obj


def divtime(l, r):
    numerator = l.days * 3600 * 24 + l.seconds
    divisor   = r.days * 3600 * 24 + r.seconds
    return int( numerator / divisor )

class SentenaiEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, str):
            return obj
        if isinstance(obj, datetime):
            return iso8601(obj) + "Z"
        if isinstance(obj, time):
            return obj.isoformat()

        print(obj)
        if type(obj) == float:
            if math.isnan(obj):
                return None
        return json.JSONEncoder.default(self, serial)

DTMIN = datetime.fromtimestamp((-2**62 + 1)/10**9).replace(tzinfo=dateutil.tz.tzutc())
DTMAX = datetime.fromtimestamp((2**62 - 1)/10**9).replace(tzinfo=dateutil.tz.tzutc())
