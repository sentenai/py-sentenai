from sentenai.api import *
from sentenai.stream.events import Event

class Patterns(API):
    def __init__(self, parent):
        API.__init__(self, parent._credentials, *parent._prefix, "patterns")

    def __getitem__(self, key):
        resp = self._get(key)
        if resp.status_code == 200:
            return Pattern.from_json(self, resp.json())
        elif resp.status_code == 404:
            raise KeyError("Pattern not found.")
        else:
            raise Exception(resp.status_code)

    def __delitem__(self, key):
        x = self[key]
        if x:
            resp = self._delete(key)
            if resp.status_code not in (200, 204):
                raise Exception(resp.status_code)
        else:
            raise KeyError("Pattern does not exist")

    def create(self, name, definition, description=""):
        if not definition:
            raise ValueError("Pattern must not be empty")
        else:
            resp = self._put(name, json={"pattern": definition if isinstance(definition, str) else {'select': definition.json()}, "description": description})
            if resp.status_code in [200, 201]:
                return self[name]
            else:
                raise Exception(resp.status_code)

    def __iter__(self):
        return iter([(x['name'], Pattern.from_json(self, x)) for x in self._get().json()])

    def __repr__(self):
        return API.__repr__(self) + ".patterns"

    def __call__(self, definition):
        if isinstance(definition, str):
            sj = definition
        elif isinstance(definition, bytes):
            sj = definition.decode('utf-8')
        else:
            sj = {'select': definition.json()}
        resp = self._post(json={"pattern": sj})
        if resp.status_code in [200, 201]:
            vid = resp.headers['Location']
            return Pattern(parent=self, name=vid, definition=sj, anonymous=True)
        else:
            raise Exception(resp.status_code)

class Pattern(API):
    def __init__(self, parent, name=None, description=None, definition=None, anonymous=False):
        API.__init__(self, parent._credentials, *parent._prefix, name)
        self._name = name
        self._description = description
        self._search = Search(self)
        self._definition = definition
        self._parent = parent
        self._anonymous = anonymous
        if anonymous and description is not None:
            raise ValueError("Anonymous patterns cannot have descriptions.")

    def __repr__(self):
        return repr(self._parent) + '[{}]'.format(repr(self._name))

    @property
    def description(self):
        return None if self._anonymous else self._description

    @property
    def name(self):
        return None if self._anonymous else self._name

    @property
    def search(self):
        return self._search

    @property
    def definition(self):
        return self._definition

    @definition.setter
    def definition(self, definition):
        if self._anonymous:
            raise TypeError("Cannot change definition of anonymous pattern")
        if isinstance(definition, str):
            s = definition
        else:
            s = {'select': definition.json()}
        resp = self._put(json={'pattern': s, 'description': self._description})
        if resp.status_code == 200:
            self._definition = s
            return self
        else:
            raise Exception(resp.status_code)

    @staticmethod
    def from_json(parent, x):
        return Pattern(parent, x['name'], x['description'], x['query'])


class Search(API):
    def __init__(self, parent):
        API.__init__(self, parent._credentials, *parent._prefix, "search")
        self._parent = parent

    def __getitem__(self, s):
        params = {}
        if s.start is not None:
            params["start"] = iso8601(s.start)
        if s.stop is not None:
            params["end"] = iso8601(s.stop)
        if s.step is not None:
            params["limit"] = int(s.step)

        results = []
        while True:
            resp = self._get(params=params)
            if resp.status_code == 200:
                for x in resp.json():
                    results.append(
                            Event(
                                ts=dt64(x['start']),
                                duration=dt64(x['end']) - dt64(x['start']),
                                data={
                                    'name': self._parent.name,
                                    'pattern': self._parent._definition,
                                    'description': self._parent._description
                                }
                            )
                    )
                try:
                    params['start'] = resp.headers['cursor']
                except KeyError:
                    return results

            else:
                raise Exception(resp.status_code)

