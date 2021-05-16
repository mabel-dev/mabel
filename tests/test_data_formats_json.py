"""
JSON parsing is fundamental to many of the other tests however, ensuring we
can parse some types without issue is worth testing in isolation so if it
fails, it's clear this is a failing case.

When this does fail, a large portion of other tests will also fail.
"""
import datetime
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.data.formats.json import parse, serialize
from rich import traceback

traceback.install()

will_normally_fail = {
    "string": "string",
    "number": 100,
    "date": datetime.date(2015, 6, 1),
    "datetime": datetime.datetime(1979, 9, 10, 23, 13),
    "list": ["item"],
    "is_true": False,
    "nil": None,
}

is_okay = {
    "string": "string",
    "number": 100,
    "list": ["item"],
    "is_true": False,
    "nil": None,
}

json_string = (
    '{"is_true":false,"list":["item"],"nil":null,"number":100,"string":"string"}'
)


def test_json_serialization():

    failed = False

    try:
        b = serialize(will_normally_fail)
    except:  # pragma: no cover
        failed = True

    assert not failed, "didn't process all types"
    assert isinstance(b, str), "didn't return string"


def test_json_serialization_multiline():

    explicit_no_indent = serialize(is_okay, indent=False)
    implicit_no_indent = serialize(is_okay)
    explicit_indent = serialize(is_okay, indent=True)

    assert implicit_no_indent == explicit_no_indent
    assert explicit_no_indent != explicit_indent

    assert "\n" in explicit_indent
    assert not "\n" in explicit_no_indent


def test_json_parsing():

    obj = parse(json_string)

    assert obj.get("is_true") == False
    assert obj.get("list") == ["item"]
    assert obj.get("nil", "not_there") is None
    assert obj.get("number") == 100
    assert obj.get("string") == "string"


if __name__ == "__main__":  # pragma: no cover
    test_json_serialization()
    test_json_serialization_multiline()
    test_json_parsing()

    print("okay")
