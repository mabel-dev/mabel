"""
Test the null reader
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))
from mabel import Reader
from mabel.adapters.null import NullReader
from mabel.data.internals.dictset import STORAGE_CLASS
from rich import traceback

traceback.install()


def test_can_read_null():
    data = [
        {"name": "Barney Stinson", "alter": "Lorenzo Von Matterhorn"},
        {"name": "Laszlo Cravensworth", "alter": "Jackie Daytona"},
        {"name": "Pheobe Buffay", "alter": "Regina Phalange"},
    ]

    """ensure we can read the test files"""
    reader = Reader(
        inner_reader=NullReader, dataset="", data=data, persistence=STORAGE_CLASS.MEMORY
    )
    assert reader.count() == 3
    assert sorted(reader.keys()) == ["alter", "name"]


if __name__ == "__main__":  # pragma: no cover
    test_can_read_null()

    print("okay")
