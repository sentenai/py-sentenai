from sentenai.api import API, Credentials
from sentenai.stream import Streams, Event
from sentenai.view import Views
from sentenai.pattern import Patterns

from threading import Thread
import time
from queue import Queue, Empty

__all__ = ['Client', 'Event']

class Logger(object):
    def __init__(self, queue):
        self._queue = queue

    def __call__(self, stream, event):
        self._queue.put((stream, event), block=False)


class Async(object):
    def __init__(self, workers=4):
        self._queue = Queue()
        self._workers = workers
        self._enabled = False
        self._threads = None

    def __enter__(self):
        self._threads = [Thread(target=self._logger, daemon=True) for x in range(self._workers if 0 < self._workers <= 32 else 4)]
        self.start()
        return Logger(self._queue)

    def __exit__(self, et, ev, tb):
        self.stop()

    def __call__(self, workers):
        self._workers = workers
        return self

    def __bool__(self):
        return self._enabled

    def start(self):
        self._enabled = True
        for t in self._threads:
            t.start()

    def stop(self):
        while not self._queue.empty():
            time.sleep(.1)
        self._enabled = False
        for t in self._threads:
            t.join()

    def _logger(self):
        while self._enabled:
            try:
                stream, event = self._queue.get(timeout=1/4)
            except Empty:
                continue
            retries = 0
            while retries < 5:
                try:
                    result = stream.insert(event)
                except:
                    retries += 1
                    time.sleep(retries)
                else:
                    break



class Client(API):
    def __init__(self, host, auth_key=""):
        API.__init__(self, Credentials(host, auth_key))

    def __repr__(self):
        return "Client(auth_key=\"{}\", host=\"{}\")".format(self._credentials.auth_key, self._credentials.host)

    def ping(self):
        """float: Ping Sentenai cluster's health check and get back response time in seconds."""
        t0 = time.time()
        r = self._get()
        if r.status_code == 200:
            return time.time() - t0
        else:
            raise Exception(r.status_code)

    @property
    def logger(self):
        """Async: get an asynchronous pool."""
        return Async()

    @property
    def streams(self):
        """Streams: the sub-api for streams."""
        return Streams(self)

    @property
    def views(self):
        """Views: the sub-api for views."""
        return Views(self)

    @property
    def patterns(self):
        """Patterns: the sub-api for patterns."""
        return Patterns(self)







