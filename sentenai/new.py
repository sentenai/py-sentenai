from datetime import datetime
from enum import Enum, auto
from sentenai.api import API, Credentials, iso8601
import time
from dataclasses import dataclass
from typing import Optional
import numpy as np
import cbor2


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
        return iter(sorted(s for s in r.json()))
        return DB(k)


tempest = Tempest()


def databases():
    return [DB(db) for db in sorted(tempest.api._get('db').json())]

class DB:
    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return self.name

    @property
    def paths(self):
        return Paths(self)


    def init(self, origin=datetime(1970,1,1)):
        if origin == None:
            r = tempest.api._put("db", self.name, json={'origin': None})
        else:
            r = tempest.api._put("db", self.name, json={'origin': iso8601(origin)})
        if r.status_code != 201:
            raise Exception("Could not initialize")


epoch = datetime(1970, 1, 1)

CACHE_DURATION = 1

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
    Event = 'event'

class Paths:
    def __init__(self, db):
        self.db = db

    def __getitem__(self, *path):
        resp = tempest.api._get('db', self.db.name, 'paths', *path).json()
        return Node(self.db, resp['node'])
    
    def __setitem__(self, path, p):
        tempest.api._put("db", self.db.name, 'paths', path, json=p.json())
        


class Node(object):
    def __init__(self, db, nid):
        self.db = db
        self.id = nid

    @property
    def links(self):
        return Links(self)

    @property
    def types(self):
        return Types(self)


class Stream(object):
    def __init__(self, node, type):
        self.type = type
        self.node = node

    def __len__(self):
        resp = tempest.api._head('db', self.node.db.name, 'nodes', self.node.id, 'types', self.type.value)
        if resp.status_code == 200:
            return int(resp.headers['Count'])
        else:
            raise IndexNotFound


    def insert(self, *data):
        cbor = cbor2.dumps([(int(np.timedelta64(v.offset, 'ns')), int(np.timedelta64(v.duration, 'ns')), float(v.value)) for v in data])
        resp = tempest.api._post('db', self.node.db.name, 'nodes', self.node.id, 'types', self.type.value,
                    json=cbor, headers={'Content-Type': 'application/cbor'}, raw=True)

    def __getitem__(self, params):
        resp = tempest.api._get('db', self.node.db.name, 'nodes', self.node.id, 'types', self.type.value,
                    headers={'Accept': 'application/cbor'})
        if self.type == Type.Event:
            return [Event(np.timedelta64(x['ts'], 'ns'), np.timedelta64(x['duration'], 'ns')) for x in resp.json()]
        elif self.type == Type.Float:
            return [Float(np.timedelta64(x['ts'], 'ns'), np.timedelta64(x['duration'], 'ns'), x['value']) for x in resp.json()]

    @property 
    def range(self):
        resp = tempest.api._get('db', self.node.db.name, 'nodes', self.node.id, 'types', self.type.value, 'range')
        return resp.json()
        

class Types(dict):
    def __init__(self, node):
        self.node = node
        self._types = {}
        self._age = 0

    def __call__(self, type):
        return Stream(self.node, type)

    def attach(self, type):
        resp = tempest.api._put('db', self.node.db.name, 'nodes', self.node.id, 'types', type.value)
        return Stream(self.node, type)


    @property
    def _ts(self):
        now = time.time()
        if now - self._age > CACHE_DURATION:
            ls = tempest.api._get('db', self.node.db.name, 'nodes', self.node.id, 'types').json()
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
            ls = tempest.api._get('db', self.node.db.name, 'nodes', self.node.id, 'links').json()
            self._links = {k : Node(self.node.db, nid) for k, nid in ls.items()}
        return self._links

    def __setitem__(self, key, item):
        raise NotImplemented

    def __getitem__(self, key):
        raise NotImplemented

    def __repr__(self):
        return repr(self._ls)

    def __len__(self):
        return repr(self._ls)

    def __delitem__(self):
        raise NotImplemented
    
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



