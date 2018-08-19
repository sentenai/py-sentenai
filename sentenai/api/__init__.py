from __future__ import print_function
import json
import pytz
import base64
import re, sys, time
import requests

import numpy as np
import pandas as pd

from pandas.io.json import json_normalize
from datetime import timedelta
from functools import partial
from multiprocessing.pool import ThreadPool
from threading import Lock

from sentenai.exceptions import *
from sentenai.exceptions import handle
from sentenai.utils import *
from sentenai.historiQL import EventPath, Returning, delta, Delta, Query, Select

from sentenai.api.uploader import Uploader
from sentenai.api.stream import Stream, Event, StreamsView
from sentenai.api.search import Search

BoundStream = Stream

if PY3:
    string_types = str
else:
    string_types = basestring

if not PY3:
    import virtualtime

try:
    from urllib.parse import quote
except:
    from urllib import quote

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
            p = Query(Select(*args)[self.timerange] if self.timerange else Select(*args))
            return Search(self.client, p)
        finally:
            self.query = None
            self.timerange = None


class BaseClient(object):
    def __init__(self, auth_key="", host="https://api.sentenai.com"):
        self.auth_key = auth_key
        self.host = host
        self.build_url = partial(build_url, self.host)
        self.session = requests.Session()
        self.session.headers.update({ 'auth-key': auth_key })
        self.select = SQ(self)

    @staticmethod
    def _debug(enable=True):
        """Toggle connection debugging."""
        import logging
        try:
            import http.client as http_client
        except ImportError:
            # Python 2
            import httplib as http_client
        http_client.HTTPConnection.debuglevel = 1
        logging.basicConfig()
        logging.getLogger().setLevel(logging.DEBUG)
        requests_log = logging.getLogger("requests.packages.urllib3")
        requests_log.setLevel(logging.DEBUG)
        requests_log.propagate = enable


    def __str__(self):
        """Return a string representation of the object."""
        return repr(self)


    def __repr__(self):
        """Return an unambiguous representation of the object."""
        return "Sentenai(auth_key='{}', server='{}')".format(
            self.auth_key, self.host)



class Sentenai(BaseClient):
    def __init__(self, auth_key="", host="https://api.sentenai.com"):
        BaseClient.__init__(self, auth_key, host)
        self.select = SQ(self)


    def Stream(self, name, *args, **kwargs):
        tz = kwargs.get('tz')
        return BoundStream(self, name, kwargs.get('meta', {}), tz, False, *args)


    def upload(self, iterable, processes=4, progress=False):
        """Takes a list of events and creates an instance of a Bulk uploader.

        Arguments:
            iterable -- an iterable object or list of events with each
                        event in this format:
                { "stream": Stream("foo) or "foo",
                  "id": "my-unique-id" (optional),
                  "ts": "2000-10-10T00:00:00Z",
                  "event": {<<event body>>}
                }
            processes -- number of processes to use. Too many processes might
                         cause a slowdown in upload speed.

        The Uploader object returned needs to be triggered with its `.start()`
        method.

        """
        ul = Uploader(self, iterable, processes)
        return ul.start(progress)


    def delete(self, stream, eid):
        """Delete event from a stream by its unique id.

        Arguments:
           stream -- A stream object corresponding to a stream stored
                     in Sentenai.
           eid    -- A unique ID corresponding to an event stored within
                     the stream.
        """
        url = self.build_url(stream, eid)
        resp = self.session.delete(url)
        status_codes(resp)


    def field_stats(self, stream, field, start=None, end=None):
        """Get stats for a given field in a stream.

       Arguments:
           stream -- A stream object corresponding to a stream stored in Sentenai.
           field  -- A dotted field name for a numeric field in the stream.
           start  -- Optional argument indicating start time in stream for calculations.
           end    -- Optional argument indicating end time in stream for calculations.
        """
        args = stream._serialized_filters()
        if start: args['start'] = start.isoformat() + ("Z" if not start.tzinfo else "")
        if end: args['end'] = end.isoformat() + ("Z" if not end.tzinfo else "")

        url = "/".join([self.host, "streams", stream.name, "fields", "event." + field, "stats"])

        resp = self.session.get(url, params=args)

        if resp.status_code == 404:
            raise NotFound('The field at "/streams/{}/fields/{}" does not exist'.format(stream.name, field))
        else:
            status_codes(resp)

        return resp.json()


    def stream_stats(self, stream):
        """Get stats for a stream.

       Arguments:
           stream -- A stream object corresponding to a stream stored in Sentenai.
        """
        args = stream._serialized_filters()
        args['stats'] = "true"
        url = "/".join([self.host, "streams", stream.name])
        resp = self.session.get(url, params=args)
        if resp.status_code == 404:
            raise NotFound('The stream "{}" does not exist'.format(stream.name))
        else:
            status_codes(resp)
        return resp.json()



    def get(self, stream, eid=None):
        """Get event or stream as JSON.

        Arguments:
           stream -- A stream object corresponding to a stream stored
                     in Sentenai.
           eid    -- A unique ID corresponding to an event stored within
                     the stream.
        """
        if eid:
            url = "/".join(
                [self.host, "streams", stream.name, "events", eid])
            resp = self.session.get(url)
        else:
            url = "/".join([self.host, "streams", stream.name])
            resp = self.session.get(url, params={'stats': True})

        if resp.status_code == 404 and eid is not None:
            raise NotFound(
                'The event at "/streams/{}/events/{}" '
                'does not exist'.format(stream.name, eid))
        elif resp.status_code == 404:
            raise NotFound(
                'The stream at "/streams/{}" '
                'does not exist'.format(stream.name, eid))
        else:
            status_codes(resp)

        if eid is not None:
            return {
                'id': resp.headers['location'],
                'ts': resp.headers['timestamp'],
                'event': resp.json()}
        else:
            return resp.json()


    def put(self, stream, event, id=None, timestamp=None, duration=None):
        """Put a new event into a stream.

        Arguments:
           stream    -- A stream object corresponding to a stream stored
                        in Sentenai.
           event     -- A JSON-serializable dictionary containing an
                        event's data
           id        -- A user-specified id for the event that is unique to
                        this stream (optional)
           timestamp -- A user-specified datetime object representing the
                        time of the event. (optional)
        """
        headers = {
            'content-type': 'application/json'
        }
        jd = event

        if timestamp and not duration:
            headers['timestamp'] = iso8601(timestamp)
        elif duration:
            headers['start'] = iso8601(timestamp)
            headers['end'] = iso8601(timestamp + duration)

        if id:
            url = '{host}/streams/{sid}/events/{eid}'.format(
                sid=stream.name, host=self.host, eid=id
            )
            resp = self.session.put(url, json=jd, headers=headers)
            if resp.status_code not in [200, 201]:
                status_codes(resp)
            else:
                return id
        else:
            url = '{host}/streams/{sid}/events'.format(
                sid=stream._name, host=self.host
            )
            resp = self.session.post(url, json=jd, headers=headers)
            if resp.status_code in [200, 201]:
                return resp.headers['location']
            else:
                status_codes(resp)
                raise APIError(resp)


    def streams(self, name=None, meta={}):
        """Get list of available streams.

        Optionally, parameters may be supplied to enable searching
        for stream subsets.

        Arguments:
           name -- A regular expression pattern to search names for
           meta -- A dictionary of key/value pairs to match from stream
                   metadata
        """
        url = "/".join([self.host, "streams"])
        resp = self.session.get(url, params={'stats': 'true'})
        status_codes(resp)

        def filtered(s):
            f = True
            if name:
                f = bool(re.search(name, s['name']))
            for k, v in meta.items():
                f = f and s.get('meta', {}).get(k) == v
            return f

        try:
            return StreamsView(self, resp.json())
        except:
            raise
            raise SentenaiException("Something went wrong")


    def destroy(self, stream, **kwargs):
        """Delete stream.

        Keyword Argument:
            confirm -- Must be `True` to confirm destroy stream
        """
        if not kwargs.get('confirm') is True:
            print("Stream not destroyed. You must confirm destroy command via argument confirm=True.")
        else:
            url = "/".join([self.host, "streams", stream.name])
            headers = {'auth-key': self.auth_key}
            resp = requests.delete(url, headers=headers)
            status_codes(resp)
            return None


    def range(self, stream, start, end, limit=None, proj=None, sorting=None):
        """Get all stream events between start (inclusive) and end (exclusive).

        Arguments:
           stream -- A stream object corresponding to a stream stored
                     in Sentenai.
           start  -- A datetime object representing the start of the requested
                     time range.
           end    -- A datetime object representing the end of the requested
                     time range.

           Result:
           A time ordered list of all events in a stream from `start` to `end`
        """
        params = stream._serialized_filters()
        params['stream'] = True
        if proj is not None:
            params['projection'] = base64json(proj)
        if limit is not None:
            params['limit'] = limit
        if sorting is not None:
            params['sort'] = sorting
        url = "/".join(
            [self.host, "streams",
             stream.name,
             "start",
             iso8601(start),
             "end",
             iso8601(end)]
        )
        resp = self.session.get(url, params=params)

        status_codes(resp)
        return [Event(self, stream, **json.loads(line)) for line in resp.text.splitlines()]


    def head(self, stream, n):
        """Get all stream events between start (inclusive) and end (exclusive).

        Arguments:
           stream -- A stream object corresponding to a stream stored
                     in Sentenai.
           start  -- A datetime object representing the start of the requested
                     time range.
           end    -- A datetime object representing the end of the requested
                     time range.

           Result:
           A time ordered list of all events in a stream from `start` to `end`
        """
        url = "/".join(
            [self.host, "streams",
             stream.name,
             "start",
             iso8601(datetime.min),
             "end",
             iso8601(datetime.max)]
        )
        params = stream._serialized_filters()
        params['limit'] = str(n)
        params['sort'] = 'asc'
        resp = self.session.get(url, params=params)
        status_codes(resp)
        return [Event(self, stream, **json.loads(line)) for line in resp.text.splitlines()]


    def tail(self, stream, n):
        """Get all stream events between start (inclusive) and end (exclusive).

        Arguments:
           stream -- A stream object corresponding to a stream stored
                     in Sentenai.
           start  -- A datetime object representing the start of the requested
                     time range.
           end    -- A datetime object representing the end of the requested
                     time range.

           Result:
           A time ordered list of all events in a stream from `start` to `end`
        """
        url = "/".join(
            [self.host, "streams",
             stream.name,
             "start",
             iso8601(datetime.min),
             "end",
             iso8601(datetime.max)]
        )
        params = stream._serialized_filters()
        params['limit'] = str(n)
        params['sort'] = 'desc'
        resp = self.session.get(url, params=params)
        status_codes(resp)
        return [Event(self, stream, **json.loads(line)) for line in resp.text.splitlines()]


    def fields(self, stream):
        """Get a list of field names for a given stream

        Argument:
           stream -- A stream object corresponding to a stream stored
                     in Sentenai.
        """
        if isinstance(stream, Stream):
            url = "/".join([self.host, "streams", stream._name, "fields"])
            params = stream._serialized_filters()
            resp = self.session.get(url, params=params)
            status_codes(resp)
            return resp.json()
        else:
            raise SentenaiException("Must be called on stream")


    def values(self, stream, timestamp=None):
        """Get all the latest values for a given stream.

        If the events in the stream don't share all their fields, this will
        forward fill values, returning the latest value for every field seen
        in the stream.

        Argument:
           stream -- A stream object corresponding to a stream stored
                     in Sentenai.
        """
        if isinstance(stream, Stream):
            url = "/".join([self.host, "streams", stream._name, "values"])
            headers = {}
            params = stream._serialized_filters()
            if timestamp:
                params['at'] = iso8601(timestamp)
            resp = self.session.get(url, params=params, headers=headers)
            status_codes(resp)
            return resp.json()
        else:
            raise SentenaiException("Must be called on stream")


    def newest(self, stream):
        """Get the most recent event in a given stream.

        Argument:
           stream -- A stream object corresponding to a stream stored
                     in Sentenai.
        """
        if isinstance(stream, Stream):
            params = stream._serialized_filters()
            url = "/".join([self.host, "streams", stream._name, "newest"])
            resp = self.session.get(url, params=params)
            status_codes(resp)
            return Event(self, stream, resp.headers['Location'], cts(resp.headers['Timestamp']), resp.json(), saved=True)
        else:
            raise SentenaiException("Must be called on stream")


    def oldest(self, stream):
        """Get the oldest event in a given stream.

        Argument:
           stream -- A stream object corresponding to a stream stored
                     in Sentenai.
        """
        if isinstance(stream, Stream):
            params = stream._serialized_filters()
            url = "/".join([self.host, "streams", stream._name, "oldest"])
            resp = self.session.get(url, params=params)
            status_codes(resp)
            return Event(self, stream, resp.headers['Location'], cts(resp.headers['Timestamp']), resp.json(), saved=True)
        else:
            raise SentenaiException("Must be called on stream")


    def unique(self, stream, field):
        params = stream._serialized_filters()
        r = self.session.get('{host}/streams/{stream}/fields/event.{field}/values'.format(host=self.host, stream=stream.name, field=field), params=params)
        return r.json()




def build_url(host, stream, eid=None):
    """Build a url for the Sentenai API.

    Arguments:
        stream -- a stream object.
        eid -- an optional event id.

    Returns:
        url -- a URL for the Sentenai API endpoint to query a stream or event
    """
    if not isinstance(stream, Stream):
        raise TypeError("stream argument must be of type sentenai.Stream")

    def with_quoter(s):
        try:
            return quote(s)
        except:
            return quote(s.encode('utf-8', 'ignore'))

    url = [host, "streams", with_quoter(stream.name)]
    events = [] if eid is None else ["events", with_quoter(eid)]
    return "/".join(url + events)
