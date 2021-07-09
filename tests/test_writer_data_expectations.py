import shutil
import os
import sys
import glob

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.adapters.disk import DiskReader, DiskWriter
from mabel.adapters.null import NullWriter
from mabel.data import SimpleWriter
from mabel.data import Reader
from rich import traceback

traceback.install()


TARGET = {
    "dataset": "_temp",
    "set_of_expectations": [
        {"expectation": "expect_column_to_exist", "column": "name"}
    ],
}


def test_validator():
    w = SimpleWriter(inner_writer=NullWriter, **TARGET)
    for i in range(int(1e5)):
        w.append({"name": "Barney Stinson", "alter": "Lorenzo Von Matterhorn"})
        w.append({"name": "Laszlo Cravensworth", "alter": "Jackie Daytona"})
    w.finalize()


if __name__ == "__main__":  # pragma: no cover
    test_validator()

    print("okay")
