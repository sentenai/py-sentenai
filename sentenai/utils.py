import dateutil, requests, numpy, sys

from datetime import datetime, timedelta, tzinfo

#### Constants

PY3 = sys.version_info[0] == 3


LEFT, CENTER, RIGHT = range(-1, 2)

DEFAULT = None

if not PY3: import virtualtime

def py2str(cls):
    """ encodes strings to utf-8 if the major version is not 3 """
    if not PY3:
        cls.__unicode__ = cls.__str__
        cls.__str__ = lambda self: self.__unicode__().encode('utf-8')
    return cls

class UTC(tzinfo):
    def utcoffset(self, dt): return timedelta()
    def dst(self, dt): return None

def iso8601(dt):
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC())
    return dt.isoformat()

def cts(ts):
    try:
        return dateutil.parser.parse(ts)
    except:
        print("invalid time: "+ts)
        return ts

def dts(obj):
    if isinstance(obj, datetime):
        serial = iso8601(obj)
        return serial
    else:
        return obj


def is_nonempty_str(s):
    isNEstr = isinstance(s, str) and not (s == '')
    try:
        isNEuni = isinstance(s, unicode) and not (s == u'')
        return isNEstr or isNEuni
    except:
        return isNEstr

def divtime(l, r):
    numerator = l.days * 3600 * 24 + l.seconds
    divisor   = r.days * 3600 * 24 + r.seconds
    return int( numerator / divisor )
