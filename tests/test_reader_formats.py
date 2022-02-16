import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel import Reader
from mabel.adapters.disk import DiskReader
from rich import traceback

traceback.install()


def test_reader_can_read_csv():
    r = Reader(
        inner_reader=DiskReader,
        dataset="tests/data/formats/csv",
        partitioning=[]
    )

    # can we read the file into dictionaries
    print(r.fetchone())
    assert r.count() == 33529, r.count()
    assert isinstance(r.fetchone(), dict)

    # are the dictionaries well-formed?
    keys = r.columns
    for row in r.i_fetchall():
        assert keys == list(row.keys()), keys


def test_reader_can_read_zipped_csv():
    r = Reader(
        inner_reader=DiskReader,
        dataset="tests/data/formats/zipped_csv",
        partitioning=[]
    )

    # can we read the file into dictionaries
    assert r.count() == 33529, r.count()
    assert isinstance(r.fetchone(), dict)

    # are the dictionaries well-formed?
    keys = r.columns
    for row in r.i_fetchall():
        assert keys == list(row.keys()), keys


if __name__ == "__main__":  # pragma: no cover
    test_reader_can_read_csv()
    test_reader_can_read_zipped_csv()

    print("okay")
