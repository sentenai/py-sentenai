from __future__ import print_function

import pytz
import re, sys, time, json
import requests

from datetime import timedelta
from multiprocessing.pool import ThreadPool
from threading import Lock

from sentenai.api.stream import Stream, Event
from sentenai.historiQL import EventPath, Returning, delta, Delta, Query
from sentenai.utils import *
from sentenai.exceptions import *
from sentenai.exceptions import handle

if PY3:
    string_types = str
else:
    string_types = basestring

if not PY3:
    import virtualtime
    from Queue import Queue
else:
    from queue import Queue

class Uploader(object):
    def __init__(self, client, iterator, processes=32):
        self.client = client
        self.iterator = iterator
        self.pool = ThreadPool(processes)
        self.succeeded = 0
        self.failed = 0
        self.lock = Lock()

    def start(self, progress=False):
        def process(data):
            def waits():
                yield 0
                wl = (0,1)
                while True:
                    wl = (wl[-1], sum(wl))
                    yield wl[-1]

            event = self.validate(data)
            if isinstance(event, tuple):
                return event

            wait = waits()
            while event:
                try:
                    self.client.put(**event)
                    with self.lock:
                        self.succeeded += 1
                    return None
                except AuthenticationError:
                    raise
                except Exception as e:
                    w = next(wait)
                    if w < 15: # 15 second wait limit
                        time.sleep(next(wait))
                    else:
                        with self.lock:
                            self.failed += 1
                        return (event, e)
                    """
                    if e.response.status_code == 400:
                        # probably bad JSON
                        return data
                    else:
                        time.sleep(next(wait))
                    """
        if progress:
            events = list(self.iterator)
            total  = len(events)
            def bar():
                sys.stderr.write("\r" * 60)
                sc = int(round(50 * self.succeeded / float(total)))
                fc = int(round(50 * self.failed    / float(total)))
                pd = (self.failed + self.succeeded) / float(total) * 100.
                sys.stderr.write(" [\033[92m{0}\033[91m{1}\033[0m{2}] {3:>6.2f}%   ".format( "#" * sc, "#" * fc, " " * (50 - sc - fc), pd))
                sys.stderr.flush()

            t0 = datetime.utcnow()
            data = self.pool.map_async(process, events)
            #sys.stderr.write("\n " + "-" * 62 + " \n")
            sys.stderr.write("\n {:<60} \n".format("Uploading {} objects:".format(total)))
            while not data.ready():
                bar()
                time.sleep(.1)
            else:
                bar()
            t1 = datetime.utcnow()
            sys.stderr.write("\n {:<60} ".format("Time elapsed: {}".format(t1 - t0)))
            sys.stderr.write("\n {:<60} ".format("Mean Obj/s: {:.1f}".format(float(total)/(t1 - t0).total_seconds())))
            sys.stderr.write("\n {:<60} \n\n".format("Failures: {}".format(self.failed)))
            #sys.stderr.write("\n " + "-" * 62 + " \n\n")
            sys.stderr.flush()
            #data.wait()
            data = data.get()
        else:
            data = self.pool.map(process, self.iterator)
        return { 'saved': self.succeeded, 'failed': list(filter(None, data)) }


    def validate(self, data):
        ts = data.get('ts')
        try:
            if isinstance(ts, string_types):
                ts = cts(ts)
            if not ts.tzinfo:
                ts = pytz.utc.localize(ts)
        except:
            return (data, "invalid timestamp")

        sid = data.get('stream')
        if isinstance(sid, Stream):
            strm = sid
        elif sid:
            strm = self.client.Stream(sid)
        else:
            return (data, "missing stream")

        try:
            evt = data['event']
        except KeyError:
            return (data, "missing event data")
        except Exception:
            return (data, "invalid event data")
        else:
            return {"stream": strm, "timestamp": ts, "id": data.get('id'), "event": evt}
