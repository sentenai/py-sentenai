from datetime import date, datetime
class Expression(object):

    @property
    def followed_by(self):
        return Function(Seq, self)

    @property
    def before(self):
        return Function(Seq, self)

    @property
    def during(self):
        return Function(During, self)

    @property
    def union(self):
        return Function(Or, self)

    @property
    def intersection(self):
        return Function(And, self)

    def __or__(self, other):
        return Or(self, other)

    def __and__(self, other):
        return And(self, other)

    def __truediv__(self, other):
        return During(self, other)

    def __rshift__(self, other):
        return Seq(self, other)


class Path(object):
    def __eq__(self, other):
        if isinstance(other, InCircle):
            return Condition(self, 'in', other)
        else:
            return Condition(self, '==', other)
    def __lt__(self, other):
        return Condition(self, '<', other)
    def __gt__(self, other):
        return Condition(self, '>', other)
    def __ne__(self, other):
        return Condition(self, '!=', other)
    def __le__(self, other):
        return Condition(self, '<=', other)
    def __ge__(self, other):
        return Condition(self, '>=', other)


class When(Expression):
    def __init__(self, signal, duration, within):
        self._signal = signal
        self._within = within
        self._duration = duration

    def json(self):
        if isinstance(self._signal, When):
            d = {'type':'serial', 
             'conds':[self._signal.json()]}
        else:
            d = self._signal.json()
        if self._duration:
            d.update(self._duration.json())
        if self._within:
            d.update(self._within.json())
        return d

class InCircle(object):
    def __init__(self, pt, radius):
        self._pt = pt
        self._r = radius

    def json(self):
        return {
            'center': { 'lon': self._pt[0], 'lat': self._pt[1] },
            'radius': self._r
        }

class Condition(When):

    def __init__(self, var, op, val):
        self._var = var
        self._op = op
        self._val = val

    def json(self):
        val = self._val
        if isinstance(self._val, float):
            vt = 'double'
        elif isinstance(self._val, bool):
            vt = 'bool'
        elif isinstance(self._val, int):
            vt = 'double'
        elif isinstance(self._val, date):
            vt = 'date'
        elif isinstance(self._val, datetime):
            vt = 'datetime'
        elif isinstance(self._val, InCircle):
            vt = 'circle'
            val = self._val.json()
        else:
            vt = 'string'
        d = {'op':self._op, 'arg':{'type':vt,  'val':val},  'type':'span'}
        d.update(self._var.json())
        return d


class Or(When):

    def __init__(self, *args):
        if len(args) == 0:
            raise ValueError("At least one condition required")
        self._args = list(args)

    def json(self):
        if len(self._args) == 1:
            return self._args[0].json()

        top = {'expr': '||', 'args': [self._args[0].json()]}
        last = top
        for x in self._args[1:-1]:
            z = {'expr': '||', 'args': [x.json()]}
            last['args'].append(z)
            last = z
        last['args'].append(self._args[-1].json())
        return top

class And(When):

    def __init__(self, *args):
        self._args = list(args)

    def json(self):
        top = {'expr': '&&', 'args': [self._args[0].json()]}
        last = top
        for x in self._args[1:-1]:
            z = {'expr': '&&', 'args': [x.json()]}
            last['args'].append(z)
            last = z
        last['args'].append(self._args[-1].json())
        return top


class During(When):

    def __init__(self, x, y):
        self._contained = x
        self._container = y

    def json(self):
        return {'type':'during', 
         'conds':[self._contained.json(), self._container.json()]}


class Either(When):

    def __init__(self, signal1, signal2):
        self._signals = [
         signal1, signal2]

    def json(self):
        return {'type':'any', 
         'conds':[s.json() for s in self._signals]}


class Any(When):

    def __init__(self, *signals):
        self._signals = signals

    def json(self):
        return {'type':'any', 
         'conds':[s.json() for s in self._signals]}


class All(When):

    def __init__(self, *signals):
        self._signals = signals

    def json(self):
        return {'type':'all', 
         'conds':[s.json() for s in self._signals]}


class Seq(When):

    def __init__(self, *signals):
        self._signals = list(signals)

    def json(self):
        if len(self._signals) == 0:
            raise ValueError("Can't have zero-length sequence")
        elif len(self._signals) == 1:
            return self._signals[0].json()
        else:
            return {'type':'serial', 
             'conds':[s.json() for s in self._signals]}

    def __getitem__(self, s):
        if isinstance(s, tuple):
            return Seq(*self._signals, *s)
        if isinstance(s, When):
            return Seq(*self._signals, *(s,))
        if isinstance(s, slice):
            return Seq(*self._signals, *(When(s.start, s.stop, s.step),))
        raise TypeError("Can't get that type")


class Function(object):

    def __init__(self, ftype, left=None):
        self._left = left
        self._type = ftype

    def __getitem__(self, slices):
        if isinstance(slices, When):
            s = [
             slices]
        elif isinstance(slices, slice):
            s = [
             When(slices.start, slices.stop, slices.step)]
        elif isinstance(slices, tuple):
            s = []
            for sl in slices:
                if type(sl) is slice:
                    s.append(When(sl.start, sl.stop, sl.step))
                elif isinstance(sl, When):
                    s.append(sl)
                else:
                    raise TypeError('Invalid type in query')

        else:
            raise TypeError('Invalid type in query')
        if len(s) == 0:
            raise ValueError('Cannot be empty')
        if self._left is None:
            if len(s) == 1:
                return s[0]
            if self._type is Seq:
                return Seq(*s)
            raise ValueError("left can't be empty")
        elif isinstance(self._type, Seq) or len(s) == 1:
            return (self._type)(self._left, *s)
        else:
            return self._type(self._left, Seq(*s))


class Within(object):

    def __init__(self, **kwargs):
        self._args = kwargs
        self._dir = ['exact', 'within', 'after']
        self._pos = 1

    def json(self):
        return {self._dir[self._pos]: self._args}

    def __invert__(self):
        self._pos *= -1
        return self


class Duration(object):

    def __init__(self, **kwargs):
        self._args = kwargs


class Max(Duration):

    def json(self):
        return {'for': {'at-most': self._args}}

    def __and__(self, other):
        if not isinstance(other, Min):
            raise ValueError('Can only combine Max with Min')
        else:
            return MinMax(self, other)


class Min(Duration):

    def json(self):
        return {'for': {'at-least': self._args}}

    def __and__(self, other):
        if not isinstance(other, Max):
            raise ValueError('Can only combine Min with Max')
        else:
            return MinMax(self, other)


class MinMax(Duration):

    def __init__(self, min, max):
        self._min = min
        self._max = max

    def json(self):
        return {'for': {'at-least':self._min.json()['for']['at-least'],  'at-most':self._max.json()['for']['at-most']}}
# okay decompiling __pycache__/signal.cpython-37.pyc
