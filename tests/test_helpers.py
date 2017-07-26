from sentenai import span, V, event

def test_span_unwrapping():
    sp1 = span(event(V.foo == 0, V.bar == 1))
    sp2 = span(sp1)
    sp3 = span(sp2)

    # FIXME: this throws a FlareSyntaxError, which means there is a bug
    # assert str(sp2) == str(sp1)
    assert len(sp2.query) == len(sp1.query)
    assert len(sp3.query) == len(sp1.query)

