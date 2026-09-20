import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel import Reader
from mabel.data import STORAGE_CLASS
from mabel.adapters.disk import DiskReader
from rich import traceback

traceback.install()


def test_reader_can_read_csv():
    r = Reader(
        inner_reader=DiskReader,
        dataset="tests/data/formats/csv",
        raw_path=True,
        persistence=STORAGE_CLASS.MEMORY,
    )

    # can we read the file into dictionaries
    print(r.first())
    assert r.count() == 33529, r.count()
    assert isinstance(r.first(), dict)

    # are the dictionaries well-formed?
    keys = r.keys(1)
    for row in r:
        assert keys == r.keys(), r.keys()


def test_reader_can_read_zipped_csv():
    r = Reader(
        inner_reader=DiskReader,
        dataset="tests/data/formats/zipped_csv",
        raw_path=True,
        persistence=STORAGE_CLASS.MEMORY,
    )

    # can we read the file into dictionaries
    assert r.count() == 33529, r.count()
    assert isinstance(r.first(), dict)

    # are the dictionaries well-formed?
    keys = r.keys(1)
    for row in r:
        assert keys == r.keys(), r.keys()


def test_reader_can_read_parquet():
    r = Reader(
        inner_reader=DiskReader,
        dataset="tests/data/formats/parquet",
        raw_path=True,
        persistence=STORAGE_CLASS.MEMORY,
    )

    # can we read the file into dictionaries
    assert r.count() == 57581, r.count()
    assert isinstance(r.first(), dict)

    # are the dictionaries well-formed?
    keys = r.keys(1)
    for row in r:
        assert keys == r.keys(), r.keys()


def test_parquet_timestamps_are_naive():
    """
    The parquet reader hands back naive datetimes. The rest of mabel compares
    timestamps against naive ones, so an aware datetime raises TypeError rather
    than filtering - which is silent until a date filter is used.
    """
    import datetime

    r = Reader(
        inner_reader=DiskReader,
        dataset="tests/data/formats/parquet",
        raw_path=True,
        persistence=STORAGE_CLASS.MEMORY,
    )

    timestamp = r.first()["timestamp"]
    assert isinstance(timestamp, datetime.datetime), type(timestamp)
    assert timestamp.tzinfo is None, timestamp
    # this is the comparison that breaks if it ever becomes aware again
    assert (timestamp < datetime.datetime.now()) in (True, False)


if __name__ == "__main__":  # pragma: no cover
    from tests.helpers.runner import run_tests

    run_tests()
