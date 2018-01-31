# coding=utf-8
import pytest
from sentenai import *
from sentenai.flare import ast_dict

def test_basic_select_span():
    s = stream("S")
    real = ast_dict(
        select().span(s.x == True)
    )
    expected = {
        "select": {
            "type": "span",
            "op": "==",
            "stream": { "name": "S" },
            "path": ( "event", "x" ),
            "arg": { "type": "bool", "val": True }
        }
    }

    assert real == expected

# TODO: figure out date/time/datetime types
def test_any_of_comparisons():
    s = stream("moose")
    real = ast_dict(
        select().span(any_of(
            span(s.x < 0),
            span(s.x >= 3.141592653589793),
            span(s.b != False)
        ))
    )

    expected = {
        "select": {
            "type": "any",
            "conds": [
                {
                    "stream": { "name": "moose" },
                    "arg": { "type": "double", "val": 0 },
                    "path": ( "event", "x" ),
                    "type": "span",
                    "op": "<"
                },
                {
                    "stream": { "name": "moose" },
                    "arg": { "type": "double", "val": 3.141592653589793 },
                    "path": ( "event", "x" ),
                    "type": "span",
                    "op": ">="
                },
                {
                    "stream": { "name": "moose" },
                    "arg": { "type": "bool", "val": False },
                    "path": ( "event", "b" ),
                    "type": "span",
                    "op": "!="
                }
            ]
        }
    }

    assert real == expected

def test_stream_access():
    s = stream("S")
    real = ast_dict(
        select()
            .span(s.even == True)
            .then(s.event == True)
            .then(s.event.event == True)
            .then(s.id == True)
            .then(s._('.id') == True)
            .then(s._('.id')._('') == True)
            .then(s._('true')._('真实') == True)
    )
    expected = {
        "select": {
            "type": "serial",
            "conds": [
                {
                    "type": "span",
                    "op": "==",
                    "arg": { "type": "bool", "val": True },
                    "path": ( "event", "even" ),
                    "stream": { "name": "S" }
                },
                {
                    "within": { "seconds": 0 },
                    "type": "span",
                    "op": "==",
                    "arg": { "type": "bool", "val": True },
                    "path": ( "event", "event" ),
                    "stream": { "name": "S" }
                },
                {
                    "within": { "seconds": 0 },
                    "type": "span",
                    "op": "==",
                    "arg": { "type": "bool", "val": True },
                    "path": ( "event", "event", "event" ),
                    "stream": { "name": "S" }
                },
                {
                    "within": { "seconds": 0 },
                    "type": "span",
                    "op": "==",
                    "arg": { "type": "bool", "val": True },
                    "path": ( "event", "id" ),
                    "stream": { "name": "S" }
                },
                {
                    "within": { "seconds": 0 },
                    "type": "span",
                    "op": "==",
                    "arg": { "type": "bool", "val": True },
                    "path": ( "event", ".id" ),
                    "stream": { "name": "S" }
                },
                {
                    "within": { "seconds": 0 },
                    "type": "span",
                    "op": "==",
                    "arg": { "type": "bool", "val": True },
                    "path": ( "event", ".id", "" ),
                    "stream": { "name": "S" }
                },
                {
                    "within": { "seconds": 0 },
                    "type": "span",
                    "op": "==",
                    "arg": { "type": "bool", "val": True },
                    "path": ( "event", "true", "真实" ),
                    "stream": { "name": "S"  }
                }
            ]
        }
    }

    assert real == expected

def test_all_any_serial():
    foo = stream("foo")
    bar = stream("bar")
    baz = stream("baz")
    qux = stream("qux")
    quux = stream("quux")

    real = ast_dict(
        select()
            .span(any_of(foo.x == True, bar.y == True))
            .then(baz.z == True)
            .then(all_of(qux._("α") == True, quux._("β") == True))
    )
    expected = {
        "select": {
            "type":"serial",
            "conds": [
                {
                    "type": "any",
                    "conds": [
                        {"op":"==", "stream": {"name": "foo"}, "path":("event","x"), "type":"span", "arg":{"type":"bool", "val":True}},
                        {"op":"==", "stream": {"name": "bar"}, "path":("event","y"), "type":"span", "arg":{"type":"bool", "val":True}}
                    ]
                },
                {
                    "within": {"seconds": 0},
                    "op":"==", "stream": {"name": "baz"}, "path":("event","z"), "type":"span", "arg":{"type":"bool", "val":True}
                },
                {
                    "within": {"seconds": 0},
                    "type": "all",
                    "conds": [
                        {"op":"==", "stream": {"name": "qux"},  "path":("event","α"), "type":"span", "arg":{"type":"bool", "val":True}},
                        {"op" :"==", "stream": {"name": "quux"}, "path":("event","β"), "type":"span", "arg":{"type":"bool", "val":True}}
                    ]
                }
            ]
        }
    }
    assert real == expected

def test_or():
    s = stream('s')
    t = stream('t')
    real = ast_dict(select().span((s.x == True) | (t.x == True)))
    expected = {
        "select": {
            "expr": "||",
            "args": [
               { "type": "span", "op": "==", "stream": {"name": "s"}, "path":("event","x"), "arg": {"type":"bool", "val":True} },
               { "type": "span", "op": "==", "stream": {"name": "t"}, "path":("event","x"), "arg": {"type":"bool", "val":True} }
            ]
        }
    }
    assert real == expected

def test_relative_span():
    s = stream('s')
    t = stream('t')
    real = ast_dict(
        select()
            .span(span(s.x == True, min=delta(years=1, months=1)) | span(t.x == True, after=delta(minutes=11), within=delta(seconds=13))
            , max=delta(weeks=1))
    )
    expected = {
        "select": {
            "expr": "||",
            "args": [
                {
                    "type": "span",
                    "op": "==",
                    "stream": {"name": "s"},
                    "path": ("event","x"),
                    "arg": {"type":"bool", "val":True},
                    "for": { "at-least": { "years": 1, "months": 1 } }
                },
                {
                    "type": "span",
                    "op": "==",
                    "stream": {"name": "t"},
                    "path": ("event","x"),
                    "arg": {"type":"bool", "val":True},
                    "after": {"minutes": 11},
                    "within": {"seconds": 13}
                }
            ],
            "for": { "at-most": { "weeks": 1 } }
        }
    }
    assert real == expected

def test_nested_relative_spans():
    s = stream('S')
    real = ast_dict(
        select()
            .span(s.x < 0)
            .then(
                span(s.x == 0).then(s.x > 0, within=delta(seconds=1)),
                within=delta(seconds=2)
            )
    )
    expected = {
        "select": {
            "type": "serial",
            "conds": [
                {
                    "type":"span",
                    "op":"<",
                    "stream":{"name":"S"},
                    "path":("event","x"),
                    "arg":{"type":"double", "val":0}
                },
                {
                    "type": "serial",
                    "conds": [
                        {
                            "type":"span",
                            "op":"==",
                            "stream":{"name":"S"},
                            "path":("event","x"),
                            "arg":{"type":"double", "val":0}
                        },
                        {
                            "type":"span",
                            "op":">",
                            "stream":{"name":"S"},
                            "path":("event","x"),
                            "arg":{"type":"double", "val":0},
                            "within": {"seconds":1}
                        }
                    ],
                    "within": {"seconds":2}
                }
            ]
        }
    }
    assert real == expected

def test_stream_filters():
    s = stream('S', V.season == "summer")
    real = ast_dict(
        select().span(span(s.temperature >= 77) & span(s.sunny == True))
    )
    expected = {
        "select": {
            "expr": "&&",
            "args": [
              {"type": "span", "op": ">=",
               "stream": {"name": "S", "filter": {"op":"==", "path":("event","season"), "arg":{"type":"string","val":"summer"}}},
               "path":("event","temperature"), "arg": {"type":"double", "val":77}
              },
              {"type": "span", "op": "==",
               "stream": {"name": "S", "filter": {"op":"==", "path":("event","season"), "arg":{"type":"string","val":"summer"}}},
               "path":("event","sunny"), "arg": {"type":"bool", "val":True}
              }
            ]
        }
    }
    assert real == expected

def test_or_stream_filters():
    s = stream('S', (V.season == "summer") | (V.season == "winter"))
    real = ast_dict(
        select().span(s.sunny == True)
    )
    expected = {
        'select': {
            'type': 'span',
            'op': '==',
            'arg': {'type': 'bool', 'val': True},
            'path': ('event', 'sunny'),
            'stream': {
                'name': 'S',
                'filter': {
                    'expr': '||',
                    'args': [
                        {'op': '==', 'arg': {'type': 'string', 'val': 'summer'}, 'type': 'span', 'path': ('event', 'season')},
                        {'op': '==', 'arg': {'type': 'string', 'val': 'winter'}, 'type': 'span', 'path': ('event', 'season')}
                    ]
                }
            }
        }
    }
    assert real == expected

def test_switches():
    s = stream('S')
    real = ast_dict(
        select().span(s(event(V.x < 0) >> event(V.x > 0)))
    )
    expected = {
        "select": {
            "type": "switch",
            "stream": { "name": "S" },
            "conds": [
                {
                    "op": "<",
                    "arg": { "type": "double", "val": 0 },
                    "type": "span",
                    "path": ( "event", "x" )
                },
                {
                    "op": ">",
                    "arg": { "type": "double", "val": 0 },
                    "type": "span",
                    "path": ( "event", "x" )
                }
            ]
        }
    }
    assert real == expected

def test_returning():
    s = stream('weather')
    real = ast_dict(
        returning(s % {
            'value': V.maxTemp,
            'other': {
                'constant': 3
            }
        })
    )
    expected = {
        'select': {'expr': True},
        'projections': {
            'explicit': [{
                'stream': {'name': 'weather'},
                'projection': {
                    'value': [{'var': ('maxTemp',)}],
                    'other': {'constant': [{'lit': {'val': 3, 'type': 'int'}}]}
                }
            }],
            '...': True
        }
    }
    assert real == expected

def test_returning_excluding():
    s = stream('weather')
    real = ast_dict(
        returning(-s)
    )
    expected = {
        'select': {'expr': True},
        'projections': {
            'explicit': [{
                'stream': {'name': 'weather'},
                'projection': False
            }],
            '...': True
        }
    }
    assert real == expected

def test_during():
    s = stream('S')
    real = ast_dict(
        select().span(during(
            s.foo == 'bar',
            s.baz > 1.5
        ))
    )
    expected = {
        "select": {
            "type": "during",
            "conds": [{
                "op": "==",
                "arg": {
                    "type": "string",
                    "val": "bar"
                },
                "type": "span",
                "path": (
                    "event",
                    "foo"
                ),
                "stream": {
                    "name": "S"
                }
            }, {
                "op": ">",
                "arg": {
                    "type": "double",
                    "val": 1.5
                },
                "type": "span",
                "path": (
                    "event",
                    "baz"
                ),
                "stream": {
                    "name": "S"
                }
            }]
        }
    }
    assert real == expected
