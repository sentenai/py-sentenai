import pytest
import re

from hypothesis            import given, assume, example
from hypothesis.strategies import text, dictionaries, booleans, integers, floats, lists, dates, datetimes, times
from sentenai              import stream, ast
from sentenai.flare        import StreamPath, Cond


def assume_parsable(query):
    assume(type(query) == Cond)
    assume(type(ast(query)) == str)


def check_syntax_with_type(query, is_multitype=False):
    s = stream("")

    assume_parsable(s.foo == query)
    assume_parsable(s.foo >  query)
    assume_parsable(s.foo >= query)
    assume_parsable(s.foo <  query)
    assume_parsable(s.foo <= query)
    assume_parsable(s.foo != query)

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

@given(booleans())
def test_boolean_conditionals(query):
    check_syntax_with_type(query)

@given(text())
def test_text_conditionals(query):
    check_syntax_with_type(query)

def test_text_wildcard():
    s = stream("")
    assume_parsable(s.foo == "test*")

def test_text_regex():
    s = stream("")
    assume_parsable(s.foo == r'')

@given(integers())
def test_int_conditionals(query):
    check_syntax_with_type(query)

@given(floats())
def test_float_conditionals(query):
    check_syntax_with_type(query)

@given(datetimes())
def test_datetimes_conditionals(query):
    check_syntax_with_type(query)

@given(dates())
def test_dates_conditionals(query):
    check_syntax_with_type(query)

# FIXME: make sure we get times as well
@given(times())
def test_times_conditionals(query):
    pass # check_syntax_with_type(query)

# FIXME: include tests for geo
"""
Geo                     -- Any geo
GeoDist (Double, Double) Double
InPoly (Double, Double) (Double, Double) (NonEmpty (Double, Double))
"""

# ================================================
# Multi-index types
# ================================================

def test_empty_list_conditional():
    check_syntax_with_type([], is_multitype=True)

@given(lists(booleans(), min_size=1))
def test_multibools_conditionals(query):
    check_syntax_with_type(query, is_multitype=True)

@given(lists(text(), min_size=1))
def test_multistrings_conditionals(query):
    check_syntax_with_type(query, is_multitype=True)

@given(lists(floats(), min_size=1))
def test_multifloat_conditionals(query):
    check_syntax_with_type(query, is_multitype=True)

@given(lists(integers(), min_size=1))
def test_multiints_conditionals(query):
    check_syntax_with_type(query, is_multitype=True)

