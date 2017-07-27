from hypothesis import given, example, assume
from hypothesis.strategies import text

from sentenai.utils import py2str, PY3
import string

@given(text())
@example(string.ascii_letters)
def test_py2str(s):

    @py2str
    class Foo:
        def __init__(self, s):
            self.s = s

        def __str__(self):
            return self.s

    assume(isinstance(str(Foo(s)), str))


