from datetime import datetime
from enum import Enum, auto
from sentenai.api import API, Credentials, iso8601
import time
from dataclasses import dataclass
from typing import Optional
import numpy as np
import cbor2
import uuid, io
import pathlib


class APIException(Exception): pass
class IndexNotFound(APIException): pass



@dataclass
class Event:
    offset: np.timedelta64
    duration: np.timedelta64

    def start(self, origin: Optional[np.datetime64] = None):
        if origin:
            return origin + self.offset
        else:
            return self.offset

    def end(self, origin: Optional[np.datetime64] = None):
        if origin:
            return origin + self.offset + self.duration
        else:
            return self.offset + self.duration

@dataclass
class Float(Event):
    value: float

@dataclass
class Int(Event):
    value: int

@dataclass
class Bool(Event):
    value: bool

@dataclass
class Text(Event):
    value: str

@dataclass
class Point(Event):
    value: (float, float)

@dataclass
class Point3(Event):
    value: (float, float, float)


class Tempest(object):
    def __init__(self, host='localhost', port=7280):
        self.host = host
        self.port = port
        self.protocol = 'http://'

    @property
    def api(self):
        h = f"{self.protocol}{self.host}:{self.port}"
        return API(Credentials(h, None))

    def __getitem__(self, k):
        return DB(self.api, k)

    @property
    def dbs(self):
        return [DB(db, dt64(o)) for db, o in sorted(self.api._get('db').json().items())]

    def tspl(self, tspl):
        return TSPL(self.api, tspl)

tempest = Tempest()

class DB:
    def __init__(self, api, name, origin=False):
        self.name = name
        self.api = api
        self._origin = origin

    def __repr__(self):
        return self.name

    @property
    def origin(self):
        if self._origin == False:
            r = self.api._get("db", self.name)
            o = r.json()['origin']
            if o is not None:
                self._origin = dt64(o)
            else:
                self._origin = None
        return self._origin

    @property
    def links(self):
        return Links(self)

    @property
    def paths(self):
        return Paths(self)

    @property
    def graph(self):
        return Graph(self)

    def init(self, origin=datetime(1970,1,1)):
        if origin == None:
            r = self.api._put("db", self.name, json={'origin': None})
        else:
            r = self.api._put("db", self.name, json={'origin': iso8601(origin)})
        if r.status_code != 201:
            raise Exception("Could not initialize")


epoch = datetime(1970, 1, 1)

CACHE_DURATION = 1

class TSPL:
    def __init__(self, api, src):
        self.api = api
        self._src = src
        self._explain = None


    def __getitem__(self, s):
        params = {}
        if s.start:
            params['start'] = s.start
        if s.stop:
            params['end'] = s.stop
        if s.step:
            params['limit'] = s.step
        return self.api._post('tspl', json=self._src, params=params).json()

    @property
    def explain(self):
        if not self._explain:
            self._explain = self.api._post('tspl/debug', json=self._src).json()
        return self._explain

    @property
    def range(self):
        return self.api._post('tspl/range', json=self._src).json()

    @property
    def tsvm(self):
        return ""

    @property
    def mermaid(self):
        return ""

    def diagram(self):
        from IPython.display import IFrame
         
        def js_ui(data, template, out_fn = None, out_path='.',
                  width="800px", height="600px", **kwargs):
            """Generate an IFrame containing a templated javascript package."""
            if not out_fn:
                out_fn = pathlib.Path(f"{uuid.uuid4()}.html")
                 
            # Generate the path to the output file
            out_path = pathlib.Path(out_path)
            filepath = out_path / out_fn
            # Check the required directory path exists
            filepath.parent.mkdir(parents=True, exist_ok=True)
         
            # The open "wt" parameters are: write, text mode;
            with io.open(filepath, 'wt', encoding='utf8') as outfile:
                # The data is passed in as a dictionary so we can pass different
                # arguments to the template
                outfile.write(template.format(**data))
         
            return IFrame(src=filepath, width=width, height=height)
        
        return js_ui({'src': self.explain['mermaid']}, TEMPLATE_MERMAIDJS)


def tspl(src):
    return TSPL(src)



class Kind(Enum):
    Directory = 'directory'
    Indexed = 'indexed'
    View = 'view'
    Virtual = 'view'

class Path:
    kind = Kind.Indexed

    @classmethod
    def json(cls):
        return {'kind': cls.kind.value}

class Indexed(Path):
    kind = Kind.Indexed

class Directory(Path):
    kind = Kind.Directory

class Virtual(Path):
    def __init__(self, tspl):
        self.source = tspl
    kind = Kind.Virtual


class Type(Enum):
    Int = 'int'
    Float = 'float'
    Bool = 'bool'
    Point = 'point'
    Point3 = 'point3'
    Text = 'text'
    Event = 'event'
    Date = 'date'
    Time = 'time'
    DateTime = 'datetime'
    TimeDelta = 'timedelta'

class Paths:
    def __init__(self, db):
        self.db = db

    def __getitem__(self, *path):
        resp = self.db.api._get('db', self.db.name, 'paths', *path).json()
        return Node(self.db, resp['node'])
    
    def __setitem__(self, path, p):
        self.db.api._put("db", self.db.name, 'paths', path, json=p.json())
        
class Graph:
    def __init__(self, db):
        self.db = db
        self._path = []

    def __getitem__(self, *path):
        self._path = path
        return self
    

    def tree(self, limit=None):
        p = {'view': 'tree'}
        if limit is not None:
            p['limit'] = limit
        return self.db.api._get('db', self.db.name, 'graph', *self._path, params=p).json()



class Node(object):
    def __init__(self, db, nid):
        self.db = db
        self.id = nid

    def __repr__(self):
        return f'Node("{self.id}")'

    @property
    def links(self):
        return Links(self)

    @property
    def types(self):
        return Types(self)

    @property
    def meta(self):
        return Meta(self)

class Meta(dict):
    def __init__(self, node):
        self.node = node
        self._meta = {}
        self._age = 0

    @property
    def _md(self):
        now = time.time()
        if now - self._age > CACHE_DURATION:
            self._meta = {k: v['value'] for k, v in self.db._get('db', self.node.db.name, 'nodes', self.node.id, 'meta').json().items()}
        return self._meta

    def __setitem__(self, key, val):
        if val is None:
            resp = self.node.db.api._delete('db', self.node.db.name, 'nodes', self.node.id, 'meta', key)
        else:
            if isinstance(val, bool):
                vtype = "bool"
            elif isinstance(val, datetime) or isinstance(val, np.datetime64):
                vtype = "datetime"
                val = dt64(val)
            elif isinstance(val, int):
                vtype = "int"
            elif isinstance(val, float):
                vtype = "float"
            else:
                vtype = "text"
            resp = self.node.db.api._patch('db', self.node.db.name, 'nodes', self.node.id, 'meta', json={key: {'type': vtype, 'value': val}})
        if resp.status_code not in [200, 201, 204]:
            raise Exception(resp.status_code)

    def __getitem__(self, key):
        return self._md[key]

    def __repr__(self):
        return repr(self._md)

    def __len__(self):
        return repr(self._md)

    def __delitem__(self):
        raise NotImplemented
    
    def clear(self):
        raise NotImplemented

    def copy(self):
        return Links(self.node, self._md)

    def has_key(self, key):
        return key in self._md
    
    def update(self, *args, **kwargs):
        raise NotImplemented

    def keys(self):
        return self._md.keys()
    
    def values(self):
        return self._md.values()
    
    def values(self):
        return self._md.items()
    
    def pop(self, *args):
        return self._md.pop(*args)

    def __cmp__(self, dict_):
        return cmp(self._md, dict_)

    def __contains__(self, item):
        return item in self._md
    
    def __iter__(self):
        return iter(self._md)

    def __unicode__(self):
        return unicode(repr(self._md))




class Stream(object):
    def __init__(self, node, type):
        self.type = type
        self.node = node

    def __len__(self):
        resp = self.node.db.api._head('db', self.node.db.name, 'nodes', self.node.id, 'types', self.type.value)
        if resp.status_code == 200:
            return int(resp.headers['Count'])
        else:
            raise IndexNotFound


    def insert(self, *data):
        cbor = None
        if self.type == Type.Float:
            cbor = cbor2.dumps([(int(np.timedelta64(v.offset, 'ns')), int(np.timedelta64(v.duration, 'ns')), float(v.value)) for v in data])
        elif self.type == Type.Text:
            cbor = cbor2.dumps([(int(np.timedelta64(v.offset, 'ns')), int(np.timedelta64(v.duration, 'ns')), v.value) for v in data])
        elif self.type == Type.Bool:
            cbor = cbor2.dumps([(int(np.timedelta64(v.offset, 'ns')), int(np.timedelta64(v.duration, 'ns')), v.value) for v in data])
        else:
            cbor = cbor2.dumps([(int(np.timedelta64(v.offset, 'ns')), int(np.timedelta64(v.duration, 'ns')), tuple(map(float, v.value))) for v in data])
        resp = self.node.db.api._post('db', self.node.db.name, 'nodes', self.node.id, 'types', self.type.value,
                    json=cbor, headers={'Content-Type': 'application/cbor'}, raw=True)

    def __getitem__(self, params):
        resp = self.node.db.api._get('db', self.node.db.name, 'nodes', self.node.id, 'types', self.type.value,
                    headers={'Accept': 'application/cbor'}, params=params)
        if self.type == Type.Event:
            return [Event(np.timedelta64(x['ts'], 'ns'), np.timedelta64(x['duration'], 'ns')) for x in resp.json()]
        elif self.type == Type.Float:
            return [Float(np.timedelta64(x['ts'], 'ns'), np.timedelta64(x['duration'], 'ns'), x['value']) for x in resp.json()]
        elif self.type == Type.Text:
            return [Text(np.timedelta64(x['ts'], 'ns'), np.timedelta64(x['duration'], 'ns'), x['value']) for x in resp.json()]
        elif self.type == Type.Bool:
            return [Bool(np.timedelta64(x['ts'], 'ns'), np.timedelta64(x['duration'], 'ns'), x['value']) for x in resp.json()]
        elif self.type == Type.Point:
            return [Point(np.timedelta64(x['ts'], 'ns'), np.timedelta64(x['duration'], 'ns'), tuple(x['value'])) for x in resp.json()]
        elif self.type == Type.Point3:
            return [Point3(np.timedelta64(x['ts'], 'ns'), np.timedelta64(x['duration'], 'ns'), tuple(x['value'])) for x in resp.json()]

    @property 
    def range(self):
        resp = self.node.db.api._get('db', self.node.db.name, 'nodes', self.node.id, 'types', self.type.value, 'range')
        return resp.json()
        

class Types(dict):
    def __init__(self, node):
        self.node = node
        self._types = {}
        self._age = 0

    def __call__(self, type):
        return Stream(self.node, type)

    def attach(self, type):
        resp = self.node.db.api._put('db', self.node.db.name, 'nodes', self.node.id, 'types', type.value)
        return Stream(self.node, type)


    @property
    def _ts(self):
        now = time.time()
        if now - self._age > CACHE_DURATION:
            ls = self.node.db.api._get('db', self.node.db.name, 'nodes', self.node.id, 'types').json()
            self._types = {Type(k) : Stream(self.node, Type(k)) for k in ls}
        return self._types

    def __setitem__(self, key, item):
        raise NotImplemented

    def __getitem__(self, key):
        return self._ts[key]

    def __repr__(self):
        return repr(self._ts)

    def __len__(self):
        return repr(self._ts)

    def __delitem__(self):
        raise NotImplemented
    
    def clear(self):
        raise NotImplemented

    def copy(self):
        return Links(self.node, self._ts)

    def has_key(self, key):
        return key in self._ts
    
    def update(self, *args, **kwargs):
        raise NotImplemented

    def keys(self):
        return self._ts.keys()
    
    def values(self):
        return self._ts.values()
    
    def values(self):
        return self._ts.items()
    
    def pop(self, *args):
        return self._ts.pop(*args)

    def __cmp__(self, dict_):
        return cmp(self._ts, dict_)

    def __contains__(self, item):
        return item in self._ts
    
    def __iter__(self):
        return iter(self._ts)

    def __unicode__(self):
        return unicode(repr(self._ts))

class Links(dict):
    def __init__(self, node):
        self.node = node
        self._links = {}
        self._age = 0

    @property
    def _ls(self):
        now = time.time()
        if False or now - self._age > CACHE_DURATION:
            if isinstance(self.node, DB):
                ls = self.node.db.api._get('db', self.node.name, 'links').json()
                self._links = {k : Node(self.node, nid) for k, nid in ls.items()}
            else:
                ls = self.node.db.api._get('db', self.node.db.name, 'nodes', self.node.id, 'links').json()
                self._links = {k : Node(self.node.db, nid) for k, nid in ls.items()}
        return self._links

    def __setitem__(self, key, item):
        raise NotImplemented

    def __getitem__(self, key):
        return self._ls[key]

    def __repr__(self):
        return repr(self._ls)

    def __len__(self):
        return repr(self._ls)

    def __delitem__(self, name):
        if isinstance(self.node, DB):
            self.node.db.api._delete('db', self.node.name, 'links', name)
        else:
            self.node.db.api._delete('db', self.node.db, 'nodes', self.node.id, 'links', name)
    
    def clear(self):
        raise NotImplemented

    def copy(self):
        return Links(self.node, self._ls)

    def has_key(self, key):
        return key in self._ls
    
    def update(self, *args, **kwargs):
        raise NotImplemented

    def keys(self):
        return self._ls.keys()
    
    def values(self):
        return self._ls.values()
    
    
    def pop(self, *args):
        return self._ls.pop(*args)

    def __cmp__(self, dict_):
        return cmp(self._ls, dict_)

    def __contains__(self, item):
        return item in self._ls
    
    def __iter__(self):
        return iter(self._ls)

    def items(self):
        return self._ls.items()

    def __unicode__(self):
        return unicode(repr(self._ls))



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
