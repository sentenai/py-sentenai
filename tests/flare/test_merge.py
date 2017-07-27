from datetime   import datetime
from hypothesis import assume
from sentenai   import stream, event, V, merge, span, delta, select

def test_merge_construction():
    s = stream("1")
    evt = event(V.foo == 1) >> event(V.bar == 2)

    span1 = span(s(evt), within=delta(days=1))
    span2 = span(s(evt), within=delta(days=2))
    assume( merge(span1, span2) )

