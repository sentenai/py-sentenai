from datetime import datetime
from hypothesis import assume
from sentenai import V, hql
from sentenai.api.stream import Stream
from sentenai.historiQL import merge

def stream(name, *filters):
    return Stream(None, name, {}, None, True, *filters)

def test_merge_construction():
    s = stream("1")
    evt = s.foo == 1

    span1 = hql.And(evt, hql.Within(days=1))
    span2 = hql.And(evt, hql.Within(days=2))
    assume( merge(span1, span2) )

