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
        any_of(
            span(s.x < 0),
            span(s.x >= 3.141592653589793),
            span(s.b != False)
        )
    )

    expected = {
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
                    "path": ( "event", "true", "\u771f\u5b9e" ),
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
