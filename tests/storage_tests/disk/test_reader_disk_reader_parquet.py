import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.adapters.disk import DiskReader
from mabel.data import Reader
from rich import traceback

traceback.install()


def test_can_read_parquet():
    r = Reader(
        inner_reader=DiskReader,
        row_format="pass-thru",
        dataset="tests/data/formats/parquet",
        raw_path=True,
    )

    i = 0
    for i, row in enumerate(r):
        pass

    assert i == 57580, i
    assert isinstance(row, str)


if __name__ == "__main__":  # pragma: no cover
    test_can_read_parquet()

    print("okay")
