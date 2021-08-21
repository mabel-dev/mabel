import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.adapters.disk import DiskReader
from mabel.data import Reader
from rich import traceback

traceback.install()


def test_can_read_zip():
    r = Reader(
        inner_reader=DiskReader,
        dataset="tests/data/formats/zip",
        raw_path=True,
    )

    i = -1
    for i, row in enumerate(r):
        pass

    assert i + 1 == 100000, i
    assert isinstance(row, dict)


if __name__ == "__main__":  # pragma: no cover
    test_can_read_zip()

    print("okay")
