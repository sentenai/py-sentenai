from shapely.geometry import Point
from datetime import datetime
from sentenai.api import API, dt64

class Metadata(API):
    def __init__(self, parent):
        API.__init__(self, parent._credentials, *parent._prefix, 'metadata')
        self._parent = parent

    def clear(self):
        resp = self._put(json={})
        if resp.status_code not in (200, 204):
            raise Exception(resp.status_code)

    def __iter__(self):
        resp = self._get()
        if resp.status_code == 404:
            return None
        elif resp.status_code == 200:
            data = resp.json()
            parsed = {}
            for k,v in data.items():
                if type(v) in [float, int, bool]:
                    parsed[k] = v
                elif type(v) == dict and 'lat' in v and 'lon' in v:
                    parsed[k] = Point(v['lon'], v['lat'])
                else:
                    for fmt in ["%Y-%m-%dT%H:%M:%S.%fZ","%Y-%m-%dT%H:%M:%SZ","%Y-%m-%dT%H:%M:%S","%Y-%m-%dT%H:%M:%S.%f"]:
                        try:
                            val = dt64(datetime.strptime(v, fmt))
                        except ValueError:
                            pass
                        else:
                            parsed[k] = val
                            break
                    else:
                        parsed[k] = v

            return iter(parsed.items())
        else:
            raise Exception(resp.status_code)


    def __repr__(self):
        repr(self._parent) + ".metadata"

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
        return dict(self)[key]

    def __setitem__(self, key, val):
        resp = self._put(key, json=val)
        if resp.status_code not in [200, 201]:
            raise Exception(resp.status_code)

    def __delitem__(self, key):
        resp = self._delete(key)
        if resp.status_code not in [200, 204]:
            raise Exception(resp.status_code)
