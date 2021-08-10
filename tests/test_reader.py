import pytest
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel import Reader
from mabel.data import STORAGE_CLASS
from mabel.adapters.disk import DiskReader
from rich import traceback

traceback.install()


from mabel.logging import get_logger

get_logger().setLevel(5)


def test_reader_can_read():
    r = Reader(inner_reader=DiskReader, dataset="tests/data/tweets", raw_path=True)
    assert len(list(r)) == 50


def test_unknown_format():
    with pytest.raises((TypeError)):
        r = Reader(
            inner_reader=DiskReader,
            dataset="tests/data/tweets",
            row_format="csv",
            raw_path=True,
        )


def test_reader_context():
    counter = 0
    with Reader(
        inner_reader=DiskReader,
        dataset="tests/data/tweets",
        raw_path=True,
        persistence=STORAGE_CLASS.MEMORY,
    ) as r:
        n = next(r)
        while n:
            counter += 1
            n = next(r)

    assert counter == 50


def test_reader_to_pandas():
    r = Reader(inner_reader=DiskReader, dataset="tests/data/tweets", raw_path=True)
    df = r.to_pandas()

    assert len(df) == 50


def test_threaded_reader():
    r = Reader(
        thread_count=2,
        inner_reader=DiskReader,
        dataset="tests/data/tweets",
        raw_path=True,
        persistence=STORAGE_CLASS.MEMORY,
    )

    print(r.collect())
    assert r.count() == 50, r.count()


def test_multiprocess_reader():

    # this is unreliable on windows
    if os.name != "nt":
        r = Reader(
            fork_processes=True,
            inner_reader=DiskReader,
            dataset="tests/data/tweets",
            raw_path=True,
        )
        df = r.to_pandas()
        assert len(df) == 50


if __name__ == "__main__":  # pragma: no cover
    test_reader_can_read()
    test_unknown_format()
    test_reader_context()
    test_reader_to_pandas()
    test_threaded_reader()
    test_multiprocess_reader()

    print("okay")
