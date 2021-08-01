import os
import sys

# assert False, "test records over the partition size are saved"
# assert False, "test the partitions are respected"

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.adapters.null import NullWriter
from mabel.data import SimpleWriter
from rich import traceback

traceback.install()

from mabel.data.writers.internals.blob_writer import BLOB_SIZE


def test_null_writer():
    # none of these should do anything
    nw = SimpleWriter(inner_writer=NullWriter, dataset="bucket/path")
    for i in range(1):
        nw.append({"blob": ["2"] * BLOB_SIZE})
    res = nw.finalize()
    assert res.startswith("NullWriter"), res


if __name__ == "__main__":  # pragma: no cover
    test_null_writer()

    print("okay")
