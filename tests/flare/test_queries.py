import pytest
import re
import datetime
from datetime import timedelta, datetime

from hypothesis            import given, assume, example, settings
from hypothesis.strategies import text, dictionaries, booleans, integers, floats, lists, dates, datetimes, times, one_of
from sentenai              import hql, V
from sentenai.api.stream   import Stream
from sentenai.historiQL    import StreamPath, Cond, Serial, Switch, And, Or, Select, Par, ast
from shapely.geometry      import Point, Polygon
# Hypothesis Strategies
# ========================

def all_types():
    # FIXME: add times(), having trouble with datetimes in py2 via virtualtime
    return one_of(text(), booleans(), integers(), floats(), dates())

def numeric():
    return one_of(integers(), floats())

def all_multitypes():
    # FIXME: add lists of datetime types
    return one_of(
        lists(text(), min_size=1),
        lists(booleans(), min_size=1),
        lists(integers(), min_size=1),
        lists(floats(), min_size=1))
        #lists(dates(), min_size=1),
        #lists(datetimes(), min_size=1))


# Conditional Checks
# ========================

def stream(name, *filters):
    return Stream(None, name, {}, None, True, *filters)

def assume_parsable(query):
    assume(type(query) == Cond)
    assume(type(ast(query)) == str)

def assert_parsable(query):
    assert (type(query) == Cond)
    assert (type(ast(query)) == str)


def check_syntax_with_type(query, is_multitype=False):
    s = stream("")

    assume_parsable(s.foo.bar.baz.qux == query)
    assume_parsable(s.foo.bar.baz.qux >  query)
    assume_parsable(s.foo.bar.baz.qux >= query)
    assume_parsable(s.foo.bar.baz.qux <  query)
    assume_parsable(s.foo.bar.baz.qux <= query)
    assume_parsable(s.foo.bar.baz.qux != query)

    with pytest.raises(TypeError):
        s.foo >> query

    with pytest.raises(TypeError):
        s.foo << query

    if is_multitype:
        pass
        #assume_parsable(s.foo in query)
        #with pytest.raises(TypeError):
        #    s.foo in query
    else:
        with pytest.raises(TypeError):
            s.foo in query


# Conditional Tests
# ========================
with settings(max_examples=1000, min_satisfying_examples=500):
    @given(all_types())
    def test_conditionals(query):
        check_syntax_with_type(query)

    @given(all_multitypes())
    @example([])
    def test_multibools_conditionals(query):
        check_syntax_with_type(query, is_multitype=True)

    def test_text_wildcard():
        s = stream("")
        assume_parsable(s.foo == "test*")

    def test_text_regex():
        s = stream("")
        assume_parsable(s.foo == r'')

    @given(numeric(), numeric(), numeric())
    def test_incircle(kmx, kmy, rad):
        s = stream("")
        incircle = hql.within_distance(rad, Point(kmx, kmy))
        assume_parsable(s.foo == incircle)

def test_inpoly():
    s = stream("")
    inpoly = hql.inside_region(Polygon([(0, 0), (1, 1), (1, 0)]))
    assert_parsable(s.foo == inpoly)


# Conditional Tests
# ========================

with settings(max_examples=1000, min_satisfying_examples=500):

    @given(all_types())
    def test_simple_conds(query):
        s1 = stream("1")
        s2 = stream("2")

        cond1 = s1.foo == query
        cond2 = s2.bar == query

        assume(type(cond1) is Cond)
        assume(type(cond2) is Cond)


    # @given(all_types(), all_types())
    # def test_switch_construction(query1, query2):
    #     spike = event(V.foo == query1) >> event(V.bar < query2) >> event(V.baz != query1)
    #     assume(isinstance(spike, Switch))

    #     s = stream("1")
    #     assume(isinstance(s(spike), Switch))


    @given(all_types())
    def test_overloaded_ops(query):
        s1 = stream("1")
        s2 = stream("2")

        cond1 = s2.foo == query
        cond2 = s1.foo == query

        assume(type(cond1 & cond2) == And)
        assume(type(cond1 | cond2) == Or)
        # assume(type(span1 >> span2) == Serial)


    # @given(all_types(), all_types())
    # def test_serial_construction(query1, query2):
    #     s1 = stream("1")
    #     s2 = stream("2")
    #     evt1 = event(V.foo == query1)
    #     evt2 = event(V.foo == query2)

    #     srl = span(s1(evt1)) \
    #          .then(s2(evt2), within=delta(days=5))

    #     assume(type(srl) is Serial)


    @given(all_types(), all_types())
    def test_par_construction(query1, query2):
        s1 = stream("1")
        s2 = stream("2")

        assume( isinstance(hql.Any(s1.foo == query1, s2.foo == query2), Par) )
        assume( isinstance(hql.All(s1.foo == query1, s2.foo == query2), Par) )

        # FIXME: this is not working as expected
        #assume(type(all_of(s1(evt1), s2(evt2))()) == dict)


    # @given(all_types(), all_types())
    # def test_select_construction(query1, query2):
    #     s1 = stream("1")
    #     s2 = stream("2")
    #     evt1 = event(V.foo == query1)
    #     evt2 = event(V.foo == query2)

    #     sel0 = select() \
    #         .span(s1(evt1))
    #     assume(isinstance(sel0, Select))

    #     sel1 = select() \
    #         .span(s1(evt1) >> s2(evt2))
    #     assume(isinstance(sel1, Select))

    #     sel3 = select() \
    #         .span(all_of(s1(evt1), s2(evt2)))
    #     assume(isinstance(sel3, Select))

    #     sel4 = select() \
    #         .span((s1(evt1), s2(evt2))) \
    #         .then(s2(evt1))

    #     assume(isinstance(sel4, Select))


# def test_select_with_bounds():
#     s = stream("1")
#     # FIXME: using this as a span returns a TypeError
#     evt = event(V.foo == 1)

#     sel = select(start=datetime.now(), end=datetime.now()) \
#             .span(s.evt == 1) \
#             .then(s.evt == 2)
#             #.span(s(evt) >> s(evt)) \
#             #.then(s(evt))

#     assume(isinstance(sel, Select))
#     assume(type(sel()) == dict)

#     sel = select(end=datetime.now()) \
#             .span(s.evt == 1) \
#             .then(s.evt == 2)
#             #.span(s(evt) >> s(evt)) \
#             #.then(s(evt))

#     assume(isinstance(sel, Select))
#     assume(type(sel()) == dict)

#     sel = select(start=datetime.now()) \
#             .span(s.evt == 1) \
#             .then(s.evt == 2)
#             #.span(s(evt) >> s(evt)) \
#             #.then(s(evt))

#     assume(isinstance(sel, Select))
#     assume(type(sel()) == dict)


