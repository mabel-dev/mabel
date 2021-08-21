import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.adapters.null import NullWriter
from rich import traceback

traceback.install()


def test_null_writer():
    # none of these should do anything
    nw = NullWriter(dataset="bucket/path")
    assert nw.commit(None, None).startswith("NullWriter")


if __name__ == "__main__":  # pragma: no cover
    test_null_writer()

    print("okay")
