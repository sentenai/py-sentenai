import pytest

from hypothesis            import given, assume, example
from hypothesis.strategies import text, dictionaries
from sentenai.api.stream   import Stream
from sentenai.historiQL    import StreamPath

def stream(name, *filters):
    return Stream(None, name, {}, None, True, *filters)

@given(text())
def test_named_streams(name):
    s = stream(name)
    assume(s._name == name)


@given(text())
def test_stream_equality(name):
    class StreamStub:
        _name = name
        def __init__(self):
            pass

    s1 = stream(name)
    s2 = stream(name)
    s3 = stream(name + " lies")

    assume(stream(name) == StreamStub())
    assume(s1 == s2)
    assume(not s1 is s2)
    assume(not (s1 == s3))


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
