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


if __name__ == "__main__":  # pragma: no cover
    from tests.helpers.runner import run_tests

    run_tests()
