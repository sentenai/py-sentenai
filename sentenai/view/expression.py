import json


class Expression(object):

    def __init__(self, fdict):
        self.fdict = {}
        for k, v in fdict.items():
            if isinstance(v, Formula):
                self.fdict[k] = v
            elif isliteral(v):
                self.fdict[k] = Literal(v)
            else:
                self.fdict[k] = Formula(v)

    def json(self):
        f = {}
        s = []
        for i, (k, v) in enumerate(self.fdict.items()):
            if isinstance(v, (Literal, Var)):
                f[k] = [v.json()]
            else:
                fn = 'feature-{:04d}'.format(i)
                f[k] = [{'var': ['event', fn]}]
                n = v.json(fn)
                s.append(n)

        #d = {'explicit': [{'type':'projection' if len(s) < 2 else 'outerjoin',  'fields':f,  'source':s}]}
        if len(s) > 1:
            return {'type': 'projection', 'fields': f, 'source': [{'type': 'outerjoin', 'source': s}]}
        else:
            return {'type':'projection',  'fields':f,  'source':s}


class Var(object):
    def __add__(self, other):
        return Formula(self) + other

    def __radd__(self, other):
        return other + Formula(self)

    def __sub__(self, other):
        return Formula(self) - other

    def __rsub__(self, other):
        return other - Formula(self)

    def __mul__(self, other):
        return Formula(self) * other

    def __rmul__(self, other):
        return other * Formula(self)

    def __truediv__(self, other):
        return Formula(self) / other

    def __rtruediv__(self, other):
        return other / Formula(self)

    def resample(self, freq):
        return Formula(self).resample(freq)

    def rolling(self, n, win_type=None):
        return Formula(self).rolling(n, win_type)

    def shift(self, n):
        return Formula(self).shift(n)

class Formula(object):

    def __init__(self, src):
        self._src = src
        self._shift = None

    def json(self, name='_'):
        j = self._src.json()
        d = {'var': j['path']}
        if self._shift:
            d['shift'] = self._shift
        return {'type':'projection', 
         'fields':{name: [d]}, 
         'source':[
          {'stream':j['stream'], 
           'projection':'default'}]}

    def shift(self, n):
        self._shift = n
        return self

    def resample(self, freq):
        return Resample(self, freq)

    def rolling(self, n, win_type=None):
        return Rolling(self, n, win_type)

    def __add__(self, n):
        if isinstance(n, Formula):
            return BinOp(self, '+', n)
        elif isliteral(n):
            return BinOp(self, '+', Literal(n))
        else:
            return BinOp(self, '+', Formula(n))

    def __mul__(self, n):
        if isinstance(n, Formula):
            return BinOp(self, '*', n)
        elif isliteral(n):
            return BinOp(self, '*', Literal(n))
        else:
            return BinOp(self, '*', Formula(n))

    def __rmul__(self, n):
        if isinstance(n, Formula):
            return BinOp(n, '*', self)
        elif isliteral(n):
            return BinOp(Literal(n), '*', self)
        else:
            return BinOp(Formula(n), '*', self)

    def __truediv__(self, n):
        if isinstance(n, Formula):
            return BinOp(self, '/', n)
        elif isliteral(n):
            return BinOp(self, '/', Literal(n))
        else:
            return BinOp(self, '/', Formula(n))

    def __rtruediv__(self, n):
        if isinstance(n, Formula):
            return BinOp(n, '/', self)
        elif isliteral(n):
            return BinOp(Literal(n), '/', self)
        else:
            return BinOp(Formula(n), '/', self)

    def __radd__(self, n):
        if isinstance(n, Formula):
            return BinOp(n, '+', self)
        elif isliteral(n):
            return BinOp(Literal(n), '+', self)
        else:
            return BinOp(Formula(n), '+', self)

    def __sub__(self, n):
        if isinstance(n, Formula):
            return BinOp(self, '-', n)
        elif isliteral(n):
            return BinOp(self, '-', Literal(n))
        else:
            return BinOp(self, '-', Formula(n))

    def __rsub__(self, n):
        if isinstance(n, Formula):
            return BinOp(n, '-', self)
        elif isliteral(n):
            return BinOp(Literal(n), '-', self)
        else:
            return BinOp(Formula(n), '-', self)


class Shift(Formula):

    def __init__(self, ft, n):
        self.n = n
        self.source = ft

    def json(self, name='_'):
        x = self.source.json(name)
        x['shift'] = self.n
        return x


class Resample(object):

    def __init__(self, ft, freq):
        self.source = ft
        self.freq = freq

    def max(self):
        return Resampled(self.source, self.freq, 'max')

    def min(self):
        return Resampled(self.source, self.freq, 'min')

    def mean(self):
        return Resampled(self.source, self.freq, 'mean')

    def std(self):
        return Resampled(self.source, self.freq, 'std')

    def sum(self):
        return Resampled(self.source, self.freq, 'sum')


class Resampled(Formula):

    def __init__(self, ft, freq, op):
        self.source = ft
        self.freq = freq
        self.op = op
        self.fill = 'na'
        self._offset = 0
        self._shift = None

    def offset(td):
        self._offset = td.total_seconds()
        return self

    def ffill(self):
        self.fill = 'ffill'
        return self

    def json(self, name='_'):
        j = {}
        v = {'var': ['event', '_']}
        if self._shift:
            v['shift'] = self._shift
        j['frequency'] = self.freq
        j['offset'] = self._offset
        j['type'] = 'resample'
        j['fill'] = self.fill
        j['source'] = [self.source.json()]
        j['fields'] = {name: {'expr':[v],  'aggregation':self.op}}
        return j


class Rolling(object):

    def __init__(self, ft, n, win_type='none', center=False):
        self.source = ft
        self.n = n
        self.center = center
        self.win_type = win_type

    def max(self, **kwargs):
        kwargs['type'] = self.win_type
        if self.win_type == "gaussian" and 'std' not in kwargs:
            kwargs['std'] = 1
        return Windowed(self, 'max', kwargs)

    def min(self, **kwargs):
        kwargs['type'] = self.win_type
        if self.win_type == "gaussian" and 'std' not in kwargs:
            kwargs['std'] = 1
        return Windowed(self, 'min', kwargs)

    def mean(self, **kwargs):
        kwargs['type'] = self.win_type
        if self.win_type == "gaussian" and 'std' not in kwargs:
            kwargs['std'] = 1
        return Windowed(self, 'mean', kwargs)

    def std(self, **kwargs):
        kwargs['type'] = self.win_type
        if self.win_type == "gaussian" and 'std' not in kwargs:
            kwargs['std'] = 1
        return Windowed(self, 'std', kwargs)

    def sum(self, **kwargs):
        kwargs['type'] = self.win_type
        if self.win_type == "gaussian" and 'std' not in kwargs:
            kwargs['std'] = 1
        return Windowed(self, 'sum', kwargs)


class Windowed(Formula):

    def __init__(self, rolling, op, winargs):
        self.win_args = winargs
        self.op = op
        self.r = rolling
        self._shift = None

    def json(self, name='_'):
        x = {'window':self.win_args, 
             'size':self.r.n, 
             'op':self.op, 
             'var':['event', '_'], 
             'labelPos':'center' if self.r.center else 'right'
             }
        if self._shift:
            x['shift'] = self._shift
        return {'fields':{name: [x]}, 
         'source':[
          self.r.source.json()], 
         'type':'projection'}


def isliteral(v):
    return type(v) in (float, int, str, bool)


class Literal(Formula):

    def __init__(self, val):
        self.val = val

    def shift(self):
        raise TypeError("Can't shift a literal.")

    def json(self, name='_'):
        x = self.val
        if isinstance(x, float):
            return {'lit': {'val':x,  'type':'double'}}
        if isinstance(x, int):
            return {'lit': {'val':x,  'type':'int'}}
        if isinstance(x, bool):
            return {'lit': {'val':x,  'type':'int'}}
        if isinstance(x, str):
            return {'lit': {'val':x,  'type':'str'}}
        raise TypeError('unsupported')


class BinOp(Formula):

    def __init__(self, left, op, right):
        self._left = left
        self._right = right
        self._op = op

    def json(self, name='_'):
        src = []
        if isinstance(self._left, Literal):
            lhs = self._left.json()
        else:
            lhs = {'var': ['event', '_lhs']}
            src.append(self._left.json('_lhs'))
        if isinstance(self._right, Literal):
            rhs = self._right.json()
        else:
            rhs = {'var': ['event', '_rhs']}
            src.append(self._right.json('_rhs'))
        if len(src) > 1:
            src = [{'source': src, 'type': 'outerjoin'}]
        return {'fields':{name: [{'lhs':lhs,  'rhs':rhs,  'op':self._op}]},  'source':src,  'type': 'projection'}
