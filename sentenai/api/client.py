import base64
import logging
import pytz
import re
import requests
import sys
import time

import http.client as http_client
import pandas as pd
import simplejson as JSON

from copy import copy
from sentenai.exceptions import *
from sentenai.utils import base64json, SentenaiEncoder
from sentenai.api.stream import Stream
#from sentenai.api.query import Query
from sentenai.historiQL import EventPath, Returning, delta, Delta, Query, Select
from sentenai.api.search import Search
from threading import Thread
from queue import Queue

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



class BaseClient(object):
    def __init__(self, auth_key, host):
        self.auth_key = auth_key
        self.host = host
        self.session = requests.Session()
        self.session.headers.update({ 'auth-key': auth_key })
        self.debug = Debug()

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
        return "Sentenai(auth_key='{}', host='{}')".format(
            self.auth_key, self.host)

    def _req(self, method, parts, params={}, headers={}, data=None):
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
            r = method("/".join([self.host]+list(parts)), params=ps, headers=headers)
        else:
            r = method("/".join([self.host]+list(parts)), params=ps, headers=headers, data=JSON.dumps(data, ignore_nan=True, cls=SentenaiEncoder))

        return self.debug.cache(r)

    def get(self, *parts, params={}, headers={}):
        return self._req(self.session.get, parts, params, headers)

    def put(self, *parts, params={}, headers={}, json={}):
        return self._req(self.session.put, parts, params, headers, data=json)

    def post(self, *parts, params={}, headers={}, json={}):
        return self._req(self.session.post, parts, params, headers, data=json)

    def delete(self, *parts, params={}, headers={}):
        return self._req(self.session.delete, parts, params, headers)

    def patch(self, *parts, params={}, headers={}, json={}):
        return self._req(self.session.patch, parts, params, headers, data=json)

class SQ(object):
    def __init__(self, client):
        self.client = client
        self.timerange = None
        self.query = None

    def __getitem__(self, s):
        x = SQ(self.client)
        x.timerange = s
        return x

    def __call__(self, *args):
        try:
            p = Query(Select(*args)[self.timerange]) if self.timerange else Query(Select(*args))
            return Search(self.client, p, start=self.timerange.start, end=self.timerange.stop) if self.timerange else Search(self.client, p)
        finally:
            self.query = None
            self.timerange = None

    def resample(self, *args, **kwargs):
        return self().resample(*args, **kwargs)

    def agg(self, *args, **kwargs):
        return self().agg(*args, **kwargs)

    def df(self, *args, **kwargs):
        return self().df(*args, **kwargs)

class Sentenai(BaseClient):
    def __init__(self, auth_key="", host="https://api.sentenai.com", notebook=False):
        BaseClient.__init__(self, auth_key, host)
        self.notebook = bool(notebook)
        self._queue = Queue(1024)
        self._workers = [Thread(target=self._logger) for x in range(2)]
        for t in self._workers: t.start()
        self.select = SQ(self)

    @property
    def where(self):
        return Query(self)

    def _logger(self):
        while True:
            evt = self._queue.get()
            while True:
                try:
                    evt.create()
                except:
                    print(evt)
                    time.sleep(1)
                else:
                    break



    def Stream(self, id, *filters):
        return Stream(self, id, filters)

    def __getitem__(self, s):
        """Get stream via dict-like reference. Throws KeyError if it doesn't exist."""
        if type(s) == str:
            x = self.get('streams', s)
            if x.status_code == 200:
                return Stream(self, s, None)
            else:
                raise KeyError("Stream `{}` not found".format(s))
        elif type(s) == tuple and len(s) == 2:
            x = self.get('streams', s[0])
            if x.status_code == 200:
                return Stream(self, s, s[1])
            else:
                raise KeyError("Stream `{}` not found".format(s))
        else:
            raise TypeError()

    def streams(self, q=None):
        """Get list of available streams.

        Optionally, parameters may be supplied to enable searching
        for stream subsets.

        Arguments:
           q -- A metadata expression to search for
        """
        return StreamsView(self, q, stats=True)

    def __call__(self, *args, **kwargs):
        """Get list of available streams.

        Optionally, parameters may be supplied to enable searching
        for stream subsets.

        Arguments:
           q -- A metadata expression to search for
        """
        return self.streams(*args, **kwargs)

    def destroy(self, stream):
        """Delete stream and all its data."""
        resp = self.delete('streams', stream.id)
        if resp.status_code in [200, 204]:
            return None
        elif resp.status_code == 404:
            raise KeyError
        else:
            raise SentenaiException(resp.status_code)

    def __delitem__(self, s):
        if isinstance(s, slice):
            raise TypeError("unhashable type: 'slice'")
        else:
            self.destroy(self.Stream(id=s))

class StreamsView(object):
    def __init__(self, client, query=None, stats=True):
        self._client = client
        self._stats = stats
        self._streams = client.get('streams', params={'stats': stats, 'q': query() if query else None}).json()

    def _repr_html_(self):
        if self._stats:
            if self._streams:
                return pd.DataFrame(self._streams)[['name', 'events', 'healthy']].rename(columns={'events': 'length'})._repr_html_()
            else:
                return pd.DataFrame(columns=["name", "length", "healthy"])._repr_html_()
        elif self._streams:
            return pd.DataFrame(self._streams)[['name']]._repr_html_()
        else:
            return pd.DataFrame(columns=["name"])._repr_html_()

    def __iter__(self):
        return iter([Stream(self._client, id=v['name']) for v in self._streams])

    def __getitem__(self, i):
        if isinstance(i, slice):
            return [Stream(self._client, x['name']) for x in self._streams[i]]
        else:
            return Stream(self._client, id=self._streams[i]['name'])





























