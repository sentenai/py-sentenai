from sentenai.api import *
from sentenai.view.expression import Expression
from numpy import datetime64, timedelta64
from sentenai.stream.events import Event


class Views(API):
    def __init__(self, parent):
        API.__init__(self, parent._credentials, *parent._prefix, "views")

    def __getitem__(self, key):
        resp = self._get(key)
        if resp.status_code == 200:
            return View.from_json(self, resp.json())
        elif resp.status_code == 404:
            raise KeyError("View not found.")
        else:
            raise Exception(resp.status_code)

    def __delitem__(self, key):
        x = self[key]
        if x:
            resp = self._delete(key)
            if resp.status_code not in (200, 204):
                raise Exception(resp.status_code)
        else:
            raise KeyError("View does not exist")

    def create(self, name, definition, description=""):
        if not definition:
            raise ValueError("View must not be empty")
        else:
            if type(definition) == str:
                vd = definition
            else:
                vd = Expression(definition).json()
            resp = self._put(name, json={"view": vd, "description": description})
            if resp.status_code in [200, 201]:
                return self[name]
            else:
                raise Exception(resp.status_code)

    def __iter__(self):
        return iter([(x['name'], View.from_json(self, x)) for x in self._get().json()])

    def __repr__(self):
        return API.__repr__(self) + ".views"

    def __call__(self, *fields, **definition):
        if not definition and len(fields) == 1 and type(fields[0]) == str:
            # text view
            resp = self._post(json={"view": fields[0]})
            if resp.status_code in [200, 201]:
                vid = resp.headers['Location']
                return View(parent=self, name=vid, definition=fields[0], anonymous=True)
            else:
                raise Exception(resp.status_code)

        from sentenai.stream.fields import Field
        for field in fields:
            if not isinstance(field, Field):
                raise TypeError("Default named fields must be instance of `Field` class.")
            d = definition
            for ps in field.path:
                if ps in d and isinstance(d[ps], dict):
                    d = d[ps]
                elif ps not in d:
                    d[ps] = {}
                else:
                    raise ValueError("Field named `{}` defined in both `fields` and `definition`.")
            else:
                d[ps] = field
        if not definition:
            raise ValueError("View must not be empty")
        else:
            sj = Expression(definition).json()
            resp = self._post(json={"view": sj})
            if resp.status_code in [200, 201]:
                vid = resp.headers['Location']
                return View(parent=self, name=vid, definition=sj, anonymous=True)
            else:
                raise Exception(resp.status_code)



class View(API):
    def __init__(self, parent, name=None, description=None, definition=None, anonymous=False):
        API.__init__(self, parent._credentials, *parent._prefix, name)
        self._name = name
        self._description = description
        self._data = Data(self)
        self._definition = definition
        self._parent = parent
        self._anonymous = anonymous
        if anonymous and description is not None:
            raise ValueError("Anonymous views cannot have descriptions.")

    def __repr__(self):
        return repr(self._parent) + '[{}]'.format(repr(self._name))

    @property
    def description(self):
        return None if self._anonymous else self._description

    @property
    def name(self):
        return None if self._anonymous else self._name

    @property
    def data(self):
        return self._data

    def __matmul__(self, at):
        at = dt64(at)
        z = self._data[at:at - datetime(1970,1,1):1]
        if len(z) == 0:
            return None
        else:
            z[0].ts = at
            z[0].duration = None
            return z[0]

    @property
    def definition(self):
        return self._definition

    @definition.setter
    def definition(self, definition):
        if self._anonymous:
            raise TypeError("Cannot change definition of anonymous view")
        s = Expression(definition).json()
        resp = self._put(json={'view': s, 'description': self._description})
        if resp.status_code == 200:
            self._definition = s
            return self
        else:
            raise Exception(resp.status_code)

    @staticmethod
    def from_json(parent, x):
        return View(parent, x.get('name', ''), x.get('description'))

class Data(API):
    def __init__(self, parent):
        API.__init__(self, parent._credentials, *parent._prefix, "data")

    def __getitem__(self, s):

        # handle tuple of time ranges
        if isinstance(s, (list, tuple)):
            return [self[x] for x in s]

        params = {'sort': 'asc'}
        stype = None
        if isinstance(s, Event):
            e = s
            if e.duration is None:
                raise ValueError("Cannot calculate end time.")
            else:
                params['start'] = e.ts
                params['end'] = e.ts + e.duration
        elif isinstance(s, slice) and isinstance(s.start, Event):
            e = s.start
            if e.duration is None:
                raise ValueError("Cannot calculate end time.")
            else:
                params['start'] = e.ts
                params['end'] = e.ts + e.duration
            if s.stop is not None:
                params['limit'] = s.stop
            if s.step is not None:
                raise TypeError("3 segment slice illegal with event start.")
        else:
            if s.start is None or s.stop is None:
                raise TypeError("View data *must* include valid time window")

            # handle anchored time deltas
            if isinstance(s.start, (timedelta, timedelta64)):
                if isinstance(s.stop, (datetime, datetime64)):
                    params['end'] = s.stop
                    params['start'] = s.stop + s.start
                else:
                    raise TypeError("slice.stop must be datetime type when slice.start is None or timedelta")
            elif isinstance(s.stop, (timedelta, timedelta64)):
                if isinstance(s.start, (datetime, datetime64)):
                    params['start'] = s.start
                    params['end'] = s.start + s.stop
                else:
                    raise TypeError("slice.start must be datetime type when slice.stop is None or timedelta")
            else:
                if s.start is not None:
                    if not isinstance(s.start, (datetime, datetime64)):
                        raise TypeError("slice start must be either None or datetime/timedelta.")
                    params['start'] = s.start
                if s.stop is not None:
                    if not isinstance(s.stop, (datetime, datetime64)):
                        raise TypeError("slice stop must be either None or datetime/timedelta.")
                    params['end'] = s.stop


            # reverse time order if necessary
            if 'start' in params and 'end' in params:
                if params['start'] > params['end']:
                    params['start'], params['end'] = params['end'], params['start']
                    params['sort'] = 'desc'
            elif 'start' not in params and 'end' not in params:
                raise ValueError("Slice formation must be based on datetime/timedelta values.")



            # handle row limit if included
            if isinstance(s.step, int):
                if s.step < 0:
                    raise ValueError("limit must be non-negative integer or None.")
                params['limit'] = s.step
            elif s.step is not None:
                raise ValueError("limit must be non-negative integer or None.")

        # time conversions
        if 'start' in params:
            params['start'] = iso8601(params['start'])
        if 'end' in params:
            params['end'] = iso8601(params['end'])

        # get data
        resp = self._get(params=params)
        if resp.status_code == 200:
            dat = []
            for e in resp.json()['events']:
                x = {}
                for key in e['event']:
                    x[key] = e['event'][key]
                dat.append(Event(ts=dt64(e['ts']), data=x))
            return dat
        else:
            raise Exception(resp.status_code)

