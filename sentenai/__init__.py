from sentenai.api import *
from sentenai.stream import Database
if PANDAS: import pandas as pd
from datetime import datetime
import io
import uuid
import cbor2
from pathlib import Path

import time

__all__ = ['Sentenai']

if PANDAS:
    def df(events):
        return pd.DataFrame([x.as_record() for x in events])

class Sentenai(API):
    def __init__(self, host=None, port=None, check=True, interactive=True):
        ## We do this so we can programmatically pass in host/port
        if host is None:
            host = 'localhost'
        if port is None:
            port = 7280
        protocol = 'http://'

        self.interactive = interactive

        h = f"{protocol}{host}:{port}"
        API.__init__(self, Credentials(h, None))
        if check and self.ping() > 0.5:
            print("warning: connection to this repository may be high latency or unstable.")

    def __repr__(self):
        return "Sentenai(host=\"{}\")".format(self._credentials.host)

    def __call__(self, tspl):
        return View(self, {'value': tspl}, when=None)

    def __iter__(self):
        r = self._get('db')
        return iter(sorted(s for s in r.json()))

    def keys(self):
        return iter(self)
   
    def items(self):
        return iter([(k, self[k]) for k in self])

    def values(self):
        return iter([self[k] for k in self])

    def init(self, name, origin=datetime(1970,1,1,0,0)):
        """Initialize a new stream database. The origin is the earliest
        point in time storable in the database. Data may not come before that time.
        If `None` is provided, then all updates must be logged with integer timestamps
        representing nanoseconds since the origin `0`.
        """
        if origin == None:
            r = self._put("db", name, json={'origin': None})
        else:
            r = self._put("db", name, json={'origin': iso8601(origin)})
        if r.status_code != 201:
            raise Exception("Could not initialize")
        return self[name]

    def ping(self):
        """Ping Sentenai get back response time in seconds."""
        t0 = time.time()
        r = self._get()
        return time.time() - t0

    def __delitem__(self, name):
        """Delete a stream database"""
        self[name]._delete()

    def __getitem__(self, db):
        """Get a stream database."""
        x = self._get('db', db)
        if x.status_code != 200:
            raise KeyError(f"`{db}` not found in {self!r}.")
        return Database(self, db, dt64(x.json().get('origin')))

    
    if PANDAS:
        def df(self, tspl=None, when=None, **tspls):
            """Dataframe"""
            if not tspl and not tspls:
                raise Exception("no arguments")
            if tspl and tspls:
                raise Exception("can't define both string TSPL and multiple TSPL statements together.")
            elif tspl:
                return View(self, {'value': tspl}, when, df=True)
            else:
                return View(self, tspls, when, df=True)



class View(API):
    def __init__(self, parent, tspl, when=None, df=False):
        self._parent = parent
        API.__init__(self, parent._credentials, *parent._prefix, "tspl")
        for key in tspl:
            if key in ['start', 'end', 'duration']:
                raise Exception("column may not be named `start`, `end`, or `duration`.")
        self._tspl = tspl
        self._when = when
        self._df = df
        self._info = None

    def __repr__(self):
        if len(self._tspl) == 1:
            return repr(list(self._tspl.values())[0])
        else:
            return "\n".join([f'{key} = {value!r}' for key, value in self._tspl.items()])
    
    def explain(self):
        return self._post("debug", json=self._tspl['value']).json()

    def plan(self):
        from IPython.display import IFrame
         
        def js_ui(data, template, out_fn = None, out_path='.',
                  width="800px", height="600px", **kwargs):
            """Generate an IFrame containing a templated javascript package."""
            if not out_fn:
                out_fn = Path(f"{uuid.uuid4()}.html")
                 
            # Generate the path to the output file
            out_path = Path(out_path)
            filepath = out_path / out_fn
            # Check the required directory path exists
            filepath.parent.mkdir(parents=True, exist_ok=True)
         
            # The open "wt" parameters are: write, text mode;
            with io.open(filepath, 'wt', encoding='utf8') as outfile:
                # The data is passed in as a dictionary so we can pass different
                # arguments to the template
                outfile.write(template.format(**data))
         
            return IFrame(src=filepath, width=width, height=height)
        
        return js_ui({'src': self.explain()['tsvm']}, TEMPLATE_MERMAIDJS)


    @property
    def range(self):
        if self._info:
            return {'start': dt64(self._info['start']), 'end': dt64(self._info['end'])}
        else:
            self._info = self._post("range", json=self._tspl['value']).json()
            return self.range

    @property
    def origin(self):
        return dt64(self.explain()['origin'])

    @property
    def type(self):
        if self._info:
            return self._info.get('type')
        else:
            self._info = self._post("range", json=self._tspl['value']).json()
            return self.type




    def __getitem__(self, i):
        params = {}

        if isinstance(i, slice):
            #params['sort'] = 'asc'

            if i.start is None:
                pass
            elif type(i.start) is int:
                params['start'] = int(i.start)
            else:
                params['start'] = iso8601(i.start)

            if i.stop is None:
                pass
            elif type(i.stop) is int:
                params['end'] = int(i.stop)
            else:
                params['end'] = iso8601(i.stop)

            if i.step is not None:
                params['limit'] = i.step
                #if i.step < 0:
                #    params['sort'] = 'desc'
            results = []
            if self._when is None and len(self._tspl) > 1:
                z = []
                for x in self._tspl.values():
                    z.append(f'events({x})')
                self._when = ' or '.join(z)

            for name, tspl in self._tspl.items():
                if self._when is None:
                    resp = self._post(json=tspl, params=params, headers={'Accept': 'application/cbor'})

                else:
                    resp = self._post(json=f'({tspl}) when {self._when}', params=params, headers={'Accept': 'application/cbor'})

                    
                t = resp.headers['type']
                if resp.headers['content-type'] == 'application/cbor':
                    o = 0
                    hasOrigin = False
                    if 'origin' in resp.headers:
                        o = int(np.datetime64(resp.headers['origin'][:-1], 'ns').astype(int))
                        hasOrigin = True
                    d = cbor2.loads(resp.content)
                    data = []
                    if hasOrigin:
                        if t != 'event':
                            for evt in d:
                                ts =  np.datetime64(o + evt[0], 'ns')
                                end = np.datetime64(o + evt[0] + evt[1], 'ns')
                                data.append({'start': ts, 'end': end, 'value': evt[2]})
                        else:
                            for evt in d:
                                ts =  np.datetime64(o + evt[0], 'ns')
                                end = np.datetime64(o + evt[0] + evt[1], 'ns')
                                data.append({'start': ts, 'end': end})
                    else:
                        if t != 'event':
                            for evt in d:
                                ts =  np.timedelta64(evt[0], 'ns')
                                end = np.timedelta64(evt[0] + evt[1], 'ns')
                                data.append({'start': ts, 'end': end, 'value': evt[2]})
                        else:
                            for evt in d:
                                ts =  np.timedelta64(evt[0], 'ns')
                                end = np.timedelta64(evt[0] + evt[1], 'ns')
                                data.append({'start': ts, 'end': end})
                    results.append(data)
                else:
                    data = resp.json()
                    if isinstance(data, list):
                        for evt in data:
                            if type(evt['start']) is int:
                                evt['start'] = np.timedelta64(evt['start'], 'ns')
                                evt['end'] = np.timedelta64(evt['end'], 'ns')
                            else:
                                evt['start'] = np.datetime64(evt['start'][:-1], 'ns')
                                evt['end'] = np.datetime64(evt['end'][:-1], 'ns')
                            if t != "event":
                                evt[name] = fromJSON(t, evt['value'])
                            if self._df:
                                evt['duration'] = evt['end'] - evt['start']
                        if self._df:
                            if t == "event":
                                results.append(pd.DataFrame(data, columns=["start", "end", "duration"]))
                            else:
                                results.append(pd.DataFrame(data, columns=["start", "end", "duration", name]))
                        else:
                            results.append(data)
                    else:
                        print(data)
                        raise Exception(data)
            if len(results) == 0:
                return None
            elif len(results) == 1:
                return results[0]
            else:
                r = results[0]
                for x in results[1:]:
                    r = pd.merge(r,x.drop(columns=['end', 'duration']),how='outer',left_on='start', right_on='start')
                if i.step:
                    return r.truncate(after=i.step-1)
                else:
                    return r


        else:
            raise Exception("wrong type")
    


TEMPLATE_MERMAIDJS="""<html>
    <head>
            <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.3.0/css/all.min.css">
    <head>
    <body>
        <script type="module">
            import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs';
            mermaid.initialize({{ startOnLoad: true }});
        </script>
 
        <pre class="mermaid">
            {src}
        </pre>
 
    </body>
</html>
"""
