from shapely.geometry import Point
from datetime import datetime
from sentenai.api import API, dt64, PANDAS
import base64
import numpy as np
if PANDAS: import pandas as pd

class Metadata(API):
    def __init__(self, parent, metadata=None):
        self._parent = parent
        API.__init__(self, parent._credentials, *parent._prefix, 'meta')

    def __iter__(self):
        return iter(k for k, v in self.items())
   
    def keys(self):
        return iter(k for k, v in self.items())

    def values(self):
        return iter(v for k, v in self.items())
    
    def items(self):
        meta = []
        for k, md in self._get().json().items():
            if md['type'] == 'int':
                meta.append((k, int(md['value'])))
            elif md['type'] == 'float':
                meta.append((k, float(md['value'])))
            elif md['type'] == 'datetime':
                meta.append((k, dt64(md['value'])))
            elif md['type'] == 'bool':
                meta.append((k, bool(md['value'])))
            else:
                meta.append((k, str(md['value'])))
        return meta


    def __repr__(self):
        return repr(self._parent) + ".meta"

    if PANDAS:
        def _repr_html_(self):
            return pd.DataFrame([
                {'key': n, 'value': v} for n, v in self.items()
            ])._repr_html_()

    def __getitem__(self, key):
        for k, v in self.items():
            if k == key:
                return v
        else:
            raise KeyError("metadata key not found")

    def __setitem__(self, key, val):
        if val is None:
            resp = self._delete(key)
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
            resp = self._patch(json={key: {'type': vtype, 'value': val}})
        if resp.status_code not in [200, 201, 204]:
            raise Exception(resp.status_code)

    def __delitem__(self, key):
        self[key] = None
