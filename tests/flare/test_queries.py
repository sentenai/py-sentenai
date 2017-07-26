import pytest

from hypothesis            import given, assume, example
from hypothesis.strategies import text, dictionaries, booleans, integers, floats, lists, dates, datetimes, times
from sentenai              import stream, ast, span, select, delta, event, V, all_of


@given(booleans())
def test_boolean_spans(query):
    s1 = stream("1")
    s2 = stream("2")

    span(s1.foo == query, s2.foo == query)
    span(s1.foo == query, s2.foo == query, within=delta(days=2))


@given(booleans(), integers())
def test_boolean_events(query, num):
    s1 = stream("1")
    s2 = stream("2")

    spike = event(V.foo == query) >> event(V.bar < num) >> event(V.baz != query)

    s1(spike)

    (span(s2.foo == 2) & span(s1.bar == 2))
    (span(s2.foo == 2) | span(s1.bar == 2))
    (span(s2.foo == 2) >> span(s1.bar == 2))

    select() \
        .span(s2(spike)) \
        .then(s2.temperatureMax < 50, within=delta(days=5))

    select() \
        .span(all_of(
                span(s1.precipType == "snow", min=delta(days=3)),
                span(s1.precipAccumulation > 8)
            ))

    select() \
        .span(s2.precipType == "snow", min=delta(days=1)) \
        .then(s2.precipType == "rain", within=delta(days=5)) \
        .then(s2.temperatureMax > 45 , within=delta(days=14), max=delta(days=5))



