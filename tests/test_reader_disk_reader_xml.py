import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.adapters.disk import DiskReader
from mabel.data import Reader
from rich import traceback

traceback.install()


def test_can_read_xml():
    r = Reader(
        inner_reader=DiskReader,
        row_format="xml",
        dataset="tests/data/formats/xml",
        raw_path=True,
    )

    i = 0
    for i, row in enumerate(r):
        print(row)
        pass

    assert i == 4, i
    assert isinstance(row, dict)

    print(row)


if __name__ == "__main__":  # pragma: no cover
    test_can_read_xml()

    print("okay")
