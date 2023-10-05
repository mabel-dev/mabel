import pytest
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel import Reader
from mabel.data import STORAGE_CLASS
from mabel.adapters.disk import DiskReader
from rich import traceback

traceback.install()


from orso.logging import get_logger

get_logger().setLevel(5)


def test_reader_can_read():
    r = Reader(inner_reader=DiskReader, dataset="tests/data/tweets", raw_path=True)
    assert len(list(r)) == 50


def test_reader_to_pandas():
    r = Reader(inner_reader=DiskReader, dataset="tests/data/tweets", raw_path=True)
    df = r.to_pandas()

    assert len(df) == 50


def test_reader_can_read_alot():
    r = Reader(inner_reader=DiskReader, dataset="tests/data/nvd", raw_path=True)
    for i, row in enumerate(r):
        if i % 1000000 == 0:
            print(i)
        pass
    assert i == 135133 or i == 16066287, i


if __name__ == "__main__":  # pragma: no cover
    from tests.helpers.runner import run_tests

    run_tests()
