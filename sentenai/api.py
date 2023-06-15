import base64
import logging
import pytz
import copy
import re
import io
import requests
import sys
import time, types
import dateutil
import dateutil.tz
from datetime import date, time, datetime, timedelta, tzinfo
import datetime as dt
from shapely.geometry import Point

import http.client as http_client
import numpy as np
import simplejson as JSON


try:
    import pandas as pd 
    PANDAS = True
except:
    pd = None
    PANDAS = False


def base64json(x):
    return base64.urlsafe_b64encode(bytes(JSON.dumps(x, ignore_nan=True, cls=SentenaiEncoder), 'UTF-8'))

def fromJSON(vtype, x):
    if vtype == "int":
        return int(x)
    elif vtype == "float":
        return float(x)
    elif vtype == "datetime":
        return dt64(x)
    elif vtype == "timedelta":
        return td64(x)
    elif vtype == "date":
        return date.fromisoformat(x)
    elif vtype == "time":
        return time.fromisoformat(x[:15])
    elif vtype.startswith("point"):
        return Point(*x)
    else:
        return x


class UTC(tzinfo):
    """A timezone class for UTC."""

    def dst(self, dt): return None

    def utcoffset(self, dt):
        """Generate a timedelta object with no offset."""
        return timedelta()

def dt64(dt):
    if dt is None:
        return None
    if isinstance(dt, str):
        if dt.endswith("Z"):
            return np.datetime64(dt[:-1])
        else:
            return np.datetime64(dt)
    elif isinstance(dt, datetime):
        return np.datetime64(dt if dt.tzinfo is None else dt.astimezone(UTC()).replace(tzinfo=None))
    elif isinstance(dt, np.datetime64):
        return dt
    elif isinstance(dt, int):
        return np.timedelta64(dt, 'ns')
    else:
        raise TypeError("Cannot convert `{}` to datetime64".format(type(dt)))

def td64(td):
    if isinstance(dt, float):
        return np.timedelta64(int(round(td*1000000000)), 'ns')
    elif isinstance(td, np.timedelta64):
        return td
    elif isinstance(td, int):
        return np.timedelta64(td, 'ns')
    elif isinstance(td, timedelta):
        return np.timedelta64(td)
    elif isinstance(td, np.timedelta64):
        return np.timedelta64(td)
    elif isinstance(td, str):
        return np.timedelta64(int(td), 'ns')
    else:
        raise TypeError("Cannot convert `{}` to timedelta64".format(type(td)))


def iso8601(dt):
    """Convert a datetime object to an ISO8601 unix timestamp."""
    if isinstance(dt, np.datetime64):
        return str(dt) + "Z"
    elif isinstance(dt, np.timedelta64):
        return str(int(dt.astype('timedelta64[ns]')))
    elif PANDAS and isinstance(dt, pd.Timestamp):
        return dt.isoformat()
    elif isinstance(dt, datetime):
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=None)
        return dt.isoformat() + "Z"
        # Virtual timestamp!
    elif isinstance(dt, int):
        return str(dt)
    else:
        raise TypeError("Cannot convert type to ISO8601")


class SentenaiEncoder(JSON.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, str):
            return obj
        if isinstance(obj, datetime):
            return iso8601(obj)
        if isinstance(obj, np.datetime64):
            return iso8601(obj)
        if PANDAS and isinstance(obj, pd.Timestamp):
            return iso8601(obj)
        if isinstance(obj, np.timedelta64):
            return str(obj.astype(int))
        if isinstance(obj, np.int64):
            return int(obj)
        if isinstance(obj, dt.time):
            return obj.isoformat()

        if type(obj) == float:
            if math.isnan(obj):
                return None
        return JSON.JSONEncoder.default(self, obj)



class Debug(object):
    def __init__(self):
        self.resp_cache = None
        self.debugging = False

    def __enter__(self):
        self.debugging = True
        http_client.HTTPConnection.debuglevel = 1
        logging.basicConfig()
        logging.getLogger().setLevel(logging.DEBUG)
        requests_log = logging.getLogger("requests.packages.urllib3")
        requests_log.setLevel(logging.DEBUG)
        requests_log.propagate = True
        return self

    def __exit__(self, *args):
        http_client.HTTPConnection.debuglevel = 0
        logging.basicConfig()
        logging.getLogger().setLevel(0)
        requests_log = logging.getLogger("requests.packages.urllib3")
        requests_log.setLevel(0)
        requests_log.propagate = False
        logging.disable(logging.CRITICAL)
        self.resp_cache = None
        self.debugging = False

    def cache(self, resp):
        if self.debugging:
            self.resp_cache = resp
        else:
            self.resp_cache = None
        return resp


class Credentials(object):
    def __init__(self, host, auth_key):
        self.host = host
        self.auth_key = auth_key

    def __repr__(self):
        return "Credentials(auth_key='{}', host='{}')".format(
            repr(self.auth_key), self.host)


class API(object):
    def __init__(self, credentials, *prefix, params={}):
        self._credentials = credentials
        self._session = requests.Session()
        a = requests.adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100)
        self._session.mount('', a)
        self._prefix = prefix
        self._params = params

    debug = Debug()

    @staticmethod
    def _debug(enable=True):
        """Toggle connection debugging."""
        http_client.HTTPConnection.debuglevel = int(enable)
        logging.basicConfig()
        logging.getLogger().setLevel(logging.DEBUG if enable else 0)
        requests_log = logging.getLogger("requests.packages.urllib3")
        requests_log.setLevel(logging.DEBUG if enable else 0)
        requests_log.propagate = enable
        if not enable:
            logging.disable()

    def __str__(self):
        """Return a string representation of the object."""
        return repr(self)

    def __repr__(self):
        """Return an unambiguous representation of the object."""
        return "API({})".format(repr(self._credentials))

    def _req(self, method, parts, params={}, headers={}, data=None, raw=False):
        params = copy.copy(params)
        params.update(self._params)
        if data and not raw:
            if isinstance(data, types.GeneratorType) or isinstance(data, io.IOBase):
                headers['Content-Type'] = 'application/x-ndjson'
            else:
                headers['Content-Type'] = 'application/json'
        ps = {}
        if self.debug.debugging:
            print("parameters\n----------")
        for k in params:
            if self.debug.debugging:
                print(k+":", params[k])
            if type(params[k]) == dict:
                ps[k] = base64json(params[k])
            elif params[k] is None:
                pass
            elif type(params[k]) is bool:
                ps[k] = str(params[k]).lower()
            else:
                ps[k] = params[k]
        if self.debug.debugging:
            print("----------\n")
        try:
            if data is None:
                r = method("/".join([self._credentials.host]+list(self._prefix)+list(parts)), params=ps, headers=headers)
            elif isinstance(data, types.GeneratorType) or isinstance(data, io.IOBase):
                r = method("/".join([self._credentials.host]+list(self._prefix)+list(parts)), params=ps, headers=headers, data=data)
            elif isinstance(data, str):
                r = method("/".join([self._credentials.host]+list(self._prefix)+list(parts)), params=ps, headers=headers, data=data)
            elif isinstance(data, bytes):
                r = method("/".join([self._credentials.host]+list(self._prefix)+list(parts)), params=ps, headers=headers, data=data)
            else:
                r = method("/".join([self._credentials.host]+list(self._prefix)+list(parts)), params=ps, headers=headers, data=JSON.dumps(data, ignore_nan=True, cls=SentenaiEncoder))
        except requests.ConnectionError:
            raise ConnectionError(f"Could not connect to sentenai repository at: `{self._credentials.host}`") from None

        resp = self.debug.cache(r)

        if resp.status_code == 400:
            x = "/".join(list(self._prefix)+list(parts))
            print("bad request:", x, data)
            raise BadRequest(f"invalid request: {resp.json()}")
        elif resp.status_code == 403:
            raise AccessDenied("Invalid credentials")
        elif resp.status_code >= 500:
            raise SentenaiError(f"Server error ({resp.status_code}): `{self._credentials.host}`\nMessage: {resp.text}")
        else:
            return resp

    def _get(self, *parts, params={}, headers={}):
        return self._req(self._session.get, parts, params, headers)

    def _put(self, *parts, params={}, headers={}, json={}):
        return self._req(self._session.put, parts, params, headers, data=json)

    def _post(self, *parts, params={}, headers={}, json={}, raw=False):
        return self._req(self._session.post, parts, params, headers, data=json, raw=raw)

    def _delete(self, *parts, params={}, headers={}):
        return self._req(self._session.delete, parts, params, headers)

    def _patch(self, *parts, params={}, headers={}, json={}):
        return self._req(self._session.patch, parts, params, headers, data=json)

    def _head(self, *parts, params={}, headers={}, json={}):
        return self._req(self._session.head, parts, params, headers)


class SentenaiException(Exception): pass
class AccessDenied(SentenaiException): pass
class ConnectionError(SentenaiException): pass
class SentenaiError(SentenaiException): pass
class BadRequest(SentenaiException): pass













