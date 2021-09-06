import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))
from mabel.data.internals.dictset import STORAGE_CLASS
from mabel.adapters.disk import DiskReader
from mabel.data import Reader
from rich import traceback

traceback.install()


def test_can_read_zip():
    r = Reader(
        inner_reader=DiskReader,
        dataset="tests/data/formats/zip",
        raw_path=True,
        persistence=STORAGE_CLASS.MEMORY,
    )

    assert r.count() == 100000, r.count()
    assert isinstance(r.first(), dict), r.first()


if __name__ == "__main__":  # pragma: no cover
    test_can_read_zip()

    print("okay")
