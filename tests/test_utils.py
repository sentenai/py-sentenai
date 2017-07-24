from hypothesis import given, example, assume
from hypothesis.strategies import text

from sentenai.utils import (py2str)
import string, unittest

@given(text())
@example(string.ascii_letters)
def test_py2str(s):
    assume(py2str(s) == s)


