import base64
import logging
import pytz
import copy
import re
import requests
import sys
import time
import dateutil
import dateutil.tz
from datetime import datetime, timedelta, tzinfo
import datetime as dt

import http.client as http_client
import numpy as np
import simplejson as JSON

def base64json(x):
    return base64.urlsafe_b64encode(bytes(JSON.dumps(x), 'UTF-8'))


class UTC(tzinfo):
    """A timezone class for UTC."""

    def dst(self, dt): return None

    def utcoffset(self, dt):
        """Generate a timedelta object with no offset."""
        return timedelta()

def dt64(dt):
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
        return dt
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
    elif isinstance(dt, datetime):
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=None)
        return dt.isoformat() + "Z"
    elif isinstance(dt, int):
        # Virtual timestamp!
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
        if isinstance(obj, dt.time):
            return obj.isoformat()

        if type(obj) == float:
            if math.isnan(obj):
                return None
        return JSON.JSONEncoder.default(self, serial)



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
            self.auth_key, self.host)


class API(object):
    def __init__(self, credentials, *prefix, params={}):
        self._credentials = credentials
        self._session = requests.Session()
        self._session.headers.update({ 'auth-key': credentials.auth_key })
        self.debug = Debug()
        self._prefix = prefix
        self._params = params

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

    def _req(self, method, parts, params={}, headers={}, data=None):
        params = copy.copy(params)
        params.update(self._params)
        if data:
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
        if data is None:
            r = method("/".join([self._credentials.host]+list(self._prefix)+list(parts)), params=ps, headers=headers)
        else:
            r = method("/".join([self._credentials.host]+list(self._prefix)+list(parts)), params=ps, headers=headers, data=JSON.dumps(data, ignore_nan=True, cls=SentenaiEncoder))

        resp = self.debug.cache(r)
        if resp.status_code == 403:
            raise AccessDenied
        else:
            return resp

    def _get(self, *parts, params={}, headers={}):
        return self._req(self._session.get, parts, params, headers)

    def _put(self, *parts, params={}, headers={}, json={}):
        return self._req(self._session.put, parts, params, headers, data=json)

    def _post(self, *parts, params={}, headers={}, json={}):
        return self._req(self._session.post, parts, params, headers, data=json)

    def _delete(self, *parts, params={}, headers={}):
        return self._req(self._session.delete, parts, params, headers)

    def _patch(self, *parts, params={}, headers={}, json={}):
        return self._req(self._session.patch, parts, params, headers, data=json)

    def _head(self, *parts, params={}, headers={}, json={}):
        return self._req(self._session.head, parts, params, headers)



class AccessDenied(Exception): pass














