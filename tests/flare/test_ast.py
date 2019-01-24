# coding=utf-8
import pytest
from sentenai import Sentenai, hql, V
from sentenai.historiQL import ast_dict, Returning, Or, Lasting, Within, After, Select
from sentenai.hql import stream

def test_basic_select_span():
    s = stream("S")
    real = ast_dict(s.x == True)
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
    real = ast_dict(hql.Any(
        s.x < 0,
        s.x >= 3.141592653589793,
        s.b != False
    ))

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
        s.even == True,
        s.event == True,
        s.event.event == True,
        s.id == True,
        s['.id'] == True,
        s['.id'][''] == True,
        s['true']['真实'] == True
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
                    "type": "span",
                    "op": "==",
                    "arg": { "type": "bool", "val": True },
                    "path": ( "event", "event" ),
                    "stream": { "name": "S" }
                },
                {
                    "type": "span",
                    "op": "==",
                    "arg": { "type": "bool", "val": True },
                    "path": ( "event", "event", "event" ),
                    "stream": { "name": "S" }
                },
                {
                    "type": "span",
                    "op": "==",
                    "arg": { "type": "bool", "val": True },
                    "path": ( "event", "id" ),
                    "stream": { "name": "S" }
                },
                {
                    "type": "span",
                    "op": "==",
                    "arg": { "type": "bool", "val": True },
                    "path": ( "event", ".id" ),
                    "stream": { "name": "S" }
                },
                {
                    "type": "span",
                    "op": "==",
                    "arg": { "type": "bool", "val": True },
                    "path": ( "event", ".id", "" ),
                    "stream": { "name": "S" }
                },
                {
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
        hql.Any(foo.x == True, bar.y == True),
        baz.z == True,
        hql.All(qux['α'] == True, quux['β'] == True)
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
                    "op":"==", "stream": {"name": "baz"}, "path":("event","z"), "type":"span", "arg":{"type":"bool", "val":True}
                },
                {
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

def test_and():
    s = stream('s')
    t = stream('t')
    real = ast_dict(s.x == True & t.x == True)
    expected = {
        "select": {
            "expr": "&&",
            "args": [
               { "type": "span", "op": "==", "stream": {"name": "s"}, "path":("event","x"), "arg": {"type":"bool", "val":True} },
               { "type": "span", "op": "==", "stream": {"name": "t"}, "path":("event","x"), "arg": {"type":"bool", "val":True} }
            ]
        }
    }
    assert real == expected

def test_or():
    s = stream('s')
    t = stream('t')
    real = ast_dict(s.x == True | t.x == True)
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

def test_order_of_operations():
    s = stream('s')
    real = ast_dict(s.x == 1 | s.y > 2 & s.z <= 3)
    expected = {
        "select": {
            "expr": "||",
            "args": [
                {
                    "type": "span",
                    "op": "==",
                    "stream": {"name": "s"},
                    "path":("event","x"),
                    "arg": {"type":"double", "val":1}
                },
                {
                    "expr": "&&",
                    "args": [
                        {
                            "type": "span",
                            "op": ">",
                            "stream": {"name": "s"},
                            "path":("event","y"),
                            "arg": {"type":"double", "val":2}
                        },
                        {
                            "type": "span",
                            "op": "<=",
                            "stream": {"name": "s"},
                            "path":("event","z"),
                            "arg": {"type":"double", "val":3}
                        }
                    ]
                }
            ]
        }
    }
    assert real == expected

def test_parens():
    s = stream('s')
    real = ast_dict((s.x == 1 | s.y > 2) & s.z <= 3)
    expected = {
        'select': {
            'expr': '&&',
            'args': [
                {
                    'expr': '||',
                    'args': [
                        {
                            'path': ('event', 'x'),
                            'arg': {'type': 'double', 'val': 1},
                            'type': 'span',
                            'stream': {'name': 's'},
                            'op': '=='
                        },
                        {
                            'path': ('event', 'y'),
                            'arg': {'type': 'double', 'val': 2},
                            'type': 'span',
                            'stream': {'name': 's'},
                            'op': '>'
                        }
                    ]
                },
                {
                    'path': ('event', 'z'),
                    'arg': {'type': 'double', 'val': 3},
                    'type': 'span',
                    'stream': {'name': 's'},
                    'op': '<='
                }
            ]
        }
    }
    assert real == expected

def test_relative_span():
    s = stream('s')
    t = stream('t')
    real = ast_dict(
        Or(
            (s.x == True, Lasting.min(years=1, months=1) ),
            (t.x == True, Within(seconds=13), After(minutes=11))
        ),
        Lasting.max(days=7)
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
            "for": { "at-most": { "days": 7 } }
        }
    }
    assert real == expected

def test_nested_relative_spans():
    s = stream('S')
    real = ast_dict(
        (s.x < 0) >> (
            ((s.x == 0) >> (s.x > 0, Within(seconds=1))),
            Within(seconds=2)
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
    real = ast_dict(s.temperature >= 77 & s.sunny == True)
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
    real = ast_dict(s.sunny == True)
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
                        {'op': '==', 'arg': {'type': 'string', 'val': 'summer'}, 'path': ('event', 'season')},
                        {'op': '==', 'arg': {'type': 'string', 'val': 'winter'}, 'path': ('event', 'season')}
                    ]
                }
            }
        }
    }
    assert real == expected

# def test_switches():
#     s = stream('S')
#     real = ast_dict(
#         select().span(s(event(V.x < 0) >> event(V.x > 0)))
#     )
#     expected = {
#         "select": {
#             "type": "switch",
#             "stream": { "name": "S" },
#             "conds": [
#                 {
#                     "op": "<",
#                     "arg": { "type": "double", "val": 0 },
#                     "path": ( "event", "x" )
#                 },
#                 {
#                     "op": ">",
#                     "arg": { "type": "double", "val": 0 },
#                     "path": ( "event", "x" )
#                 }
#             ]
#         }
#     }
#     assert real == expected

# def test_unary_switch():
#     s = stream('S')
#     real = ast_dict(
#         select().span(s(event(V.x < 0)))
#     )
#     expected = {
#         "select": {
#             "type": "switch",
#             "stream": { "name": "S" },
#             "conds": [
#                 {'expr': True},
#                 {
#                     "op": "<",
#                     "arg": { "type": "double", "val": 0 },
#                     "path": ( "event", "x" )
#                 }
#             ]
#         }
#     }
#     assert real == expected

def test_returning():
    s = stream('weather')
    real = (Returning(s % {
        'value': V.maxTemp,
        'other': {
            'constant': 3
        }
    }))()
    expected = {
        'projections': {
            'explicit': [{
                'stream': {'name': 'weather'},
                'projection': {
                    'value': [{'var': ('event', 'maxTemp')}],
                    'other': {'constant': [{'lit': {'val': 3, 'type': 'int'}}]}
                }
            }],
            '...': True
        }
    }
    assert real == expected

def test_returning_excluding():
    s = stream('weather')
    real = (
        Returning(-s)
    )()
    expected = {
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
        hql.During(
            s.foo == 'bar',
            s.baz > 1.5
        )
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
