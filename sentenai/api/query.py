
class Query(object):
    def __init__(self, client, stream=None):
        self._client = client
        self._stream = stream
        self._query = None

    def __getitem__(self, s):
        if self._query is not None:
            self._query = When.fold(s)

        return Query(self._client, self._stream, s)

class HQL(object):
    def __call__(self, name):
        self._name = name

class When(HQL):
    def __init__(self, conditions, duration, spacing, after=None):
        self._conditions = conditions
        self._duration = duration
        self._spacing = spacing
        self._after = after

    def json(self, before=None):
        # Build the JSON
        x = self._conditions.json()
        x['type'] = 'span'
        x['stream'] = self._stream.json()
        if self._duration:
            x.update(self._duration.json())
        if self._spacing:
            x.update(self._spacing.json())

        # This is the only one
        if self._after is None and before is None:
            return x

        # This is the root
        elif self._after is None:
            return {'type': 'serial', 'conds': [x, before]}

        # This is part of an existing serial
        elif before.get('type', {}) == 'serial':
            before['conds'].insert(0, x)
            return self._after.json(before)

        # This is building the base serial
        else:
            return self._after.json({'type': 'serial', 'conds': [x, before]})

    @classmethod
    def fold(cls, *args):
        after = None
        for s in args:
            if isinstance(s, HQL):
                s = slice(s)
            if type(s) != slice:
                raise TypeError
            after = cls(s.start, s.stop, s.step, after)
        return after

    @property
    def before(self):
        return Before(self)

    @property
    def followed_by(self):
        return Before(self)

    @property
    def during(self):
        return During(self)

class During(HQL):
    def __init__(self, holds):
        self._holds = holds
        self._during = None

    def __getitem__(self, during):
        if type(during) != tuple:
            self._during = When.fold(during)
        else:
            self._during = When.fold(*during)
        return self

    def json(self):
        if self._during is None:
            raise TypeError("Incomplete `during`")
        return { 'type': 'during', 'conds': [self._holds.json(), self._during.json()] }


class Before(object):
    def __init__(self, first):
        self._first = first

    def __getitem__(self, second):
        return When(second.start, second.stop, second.step, self._first)

    def json(self):
        raise TypeError("incomplete before")

