import os
import sys

import orjson

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.adapters.null import NullWriter
from mabel.data import StreamWriter
from rich import traceback

traceback.install()


DATA_SET = [
    {"key": 6, "value": ["s", "i", "x"], "combinations": 3},  # (6,s),(6,i),(6,x)
    {"key": [1, 0], "value": ["t", "e", "n"], "combinations": 6},
    {"key": 0, "value": None, "combinations": 1},
]


def test_writer_substitutions():
    w = StreamWriter(
        dataset="TEST/{key}/{value}",
        inner_writer=NullWriter,
        schema=["key", "value", "combinations"],
    )

    for record in DATA_SET:
        combinations = w.append(record)
        assert combinations == record["combinations"], combinations


if __name__ == "__main__":  # pragma: no cover
    from tests.helpers.runner import run_tests

    run_tests()
