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


def build_url(host, stream, eid=None):
    if not isinstance(stream, Stream):
        raise TypeError("stream argument must be of type sentenai.Stream")

    if not is_nonempty_str(eid):
        raise TypeError("eid argument must be a non-empty string")

    def with_quoter(s):
        try:
            return quote(s)
        except:
            return quote(s.encode('utf-8', 'ignore'))

    url    = [host, "streams", with_quoter(stream()['name'])]
    events = [] if eid is None else ["events", with_quoter(eid)]
    return "/".join(url + events)



def divtime(l, r):
    numerator = l.days * 3600 * 24 + l.seconds
    divisor   = r.days * 3600 * 24 + r.seconds
    return int( numerator / divisor )
