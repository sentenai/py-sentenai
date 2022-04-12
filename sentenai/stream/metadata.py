from shapely.geometry import Point
from datetime import datetime
from sentenai.api import API, dt64, PANDAS
import base64
if PANDAS: import pandas as pd

class Metadata(API):
    def __init__(self, parent, metadata=None):
        API.__init__(self, parent._credentials, *parent._prefix, 'metadata')
        self._parent = parent
        if hasattr(parent, '_path'):
            self._meta_prefix = "/" + base64.urlsafe_b64encode("/".join(parent._path).replace('#','&sect;&sect;').encode('utf-8')).decode('utf-8').replace("=",'')
        else:
            self._meta_prefix = ""

        self._meta_cache = metadata

    def __iter__(self):
        return iter(k for k, v in self.items())
   
    def keys(self):
        return iter(k for k, v in self.items())

    def values(self):
        return iter(v for k, v in self.items())
    
    def items(self):
        if isinstance(self._meta_cache, dict):
            data = self._meta_cache
        else:
            resp = self._get()
            if resp.status_code == 404:
                return None
            elif resp.status_code == 200:
                data = resp.json()
            else:
                raise Exception(resp.status_code)

        parsed = {}
        for k, v in data.items():
            z = k.split("#", 1)
            if len(z) != 2 or z[0] != self._meta_prefix:
                continue # doesn't match prefix
            else:
                pfx, key = z

            if type(v) in [float, int, bool]:
                parsed[key] = v
            elif type(v) == dict and 'lat' in v and 'lon' in v:
                parsed[key] = Point(v['lon'], v['lat'])
            else:
                for fmt in ["%Y-%m-%dT%H:%M:%S.%fZ","%Y-%m-%dT%H:%M:%SZ","%Y-%m-%dT%H:%M:%S","%Y-%m-%dT%H:%M:%S.%f"]:
                    try:
                        val = dt64(datetime.strptime(v, fmt))
                    except ValueError:
                        pass
                    else:
                        parsed[key] = val
                        break
                else:
                    parsed[key] = v

        return iter(parsed.items())


    def __repr__(self):
        return repr(self._parent) + ".metadata"

    if PANDAS:
        def _repr_html_(self):
            return pd.DataFrame([
                {'key': n, 'value': v} for n, v in self.items()
            ])._repr_html_()



    def _type(self, v):
        if type(v) in [int, float]:
            return "Numeric"
        elif type(v) == datetime:
            return "Datetime"
        elif type(v) == bool:
            return "Boolean"
        else:
            return "String"

    def __getitem__(self, key):
        print(key)
        for k, v in self.items():
            if k == key:
                return v
        else:
            raise KeyError("metadata key not found")

    def __setitem__(self, key, val):
        self._meta_cache = None
        resp = self._patch(json={f"{self._meta_prefix}#{key}": val})
        if resp.status_code not in [200, 201, 204]:
            raise Exception(resp.status_code)

    def __delitem__(self, key):
        self[key] = None
