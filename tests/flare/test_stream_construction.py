import pytest

from hypothesis            import given, assume, example
from hypothesis.strategies import text, dictionaries
from sentenai.api.stream   import Stream
from sentenai.historiQL    import StreamPath
from sentenai.hql          import V

def stream(name, *filters):
    return Stream(None, name, {}, None, True, *filters)

@given(text())
def test_named_streams(name):
    s = stream(name)
    assume(s.name == name)


@given(text())
def test_stream_equality(name):
    s1 = stream(name)
    s2 = stream(name)
    s3 = stream(name + " lies")

    assume(s1 == s2)
    assume(not s1 is s2)
    assume(not (s1 == s3))

@given(text())
def test_filtered_streams(name):
    s1 = stream(name)
    s2 = stream(name)
    s3 = stream(name + " lies")

    assume(s1.filtered(V.temp > 32) == stream(name, V.temp > 32))
    assume(s1.filtered(V.temp > 100) != stream(name, V.temp < 0))
    assume(s3.filtered(V.temp > 32) != stream(name, V.temp > 32))

    s4 = s1.filtered(V.temp > 1000)
    assume(s4.filtered(V.temp > 32, replace=True) == stream(name, V.temp > 32))

@given(text(min_size=1), dictionaries(text(min_size=1), text()))
def test_stream_properties(name, meta):
    s = stream(name, meta)
    assume(s.name == name)
    s._set(name+" new")
    assume(s.name == name + " new")
    assume(s.meta == meta)

@given(text(min_size=1), dictionaries(text(min_size=1), text()))
def test_happy_stream_getitems(name, meta):
    s = stream(name, meta)
    assume(s.meta == meta)
    assume(s.name == name)

@given(text(min_size=1))
def test_stream_to_string(name):
    """ TODO: there are some behaviors around str() which could be tested """
    to_str = str(stream(name))
    pass

@given(text())
@example(path="name")
@example(path="meta")
def test_stream_path_conversions(path):
    s = stream("")
    assume(type(s[path]) == StreamPath)

@given(text())
def test_stream_calls(path):
    s = stream("")
    with pytest.raises(TypeError):
       s(path)
