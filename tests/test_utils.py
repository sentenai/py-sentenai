from hypothesis import given, example, assume
from hypothesis.strategies import text

from sentenai.utils import py2str, PY3
import string, unittest

@given(text())
@example(string.ascii_letters)
def test_py2str(s):
    @py2str
    class Foo:
        def __str__(self):
            return 'im a string'

    assume(isinstance(str(s), str if PY3 else unicode))


