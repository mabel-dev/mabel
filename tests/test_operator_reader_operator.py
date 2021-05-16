"""
Test Filter Operator
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.operators import ReaderOperator
from mabel.adapters.disk import DiskReader
from rich import traceback

traceback.install()


def test_reader_operator():

    read = ReaderOperator(
        inner_reader=DiskReader, dataset="tests/data/tweets", raw_path=True
    )
    for i, (d, c) in enumerate(read.execute(None, None)):
        pass
    assert i == 49, i


if __name__ == "__main__":  # pragma: no cover
    test_reader_operator()

    print("okay")
