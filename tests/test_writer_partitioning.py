import os
import sys

# assert False, "test records over the partition size are saved"
# assert False, "test the partitions are respected"

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.adapters.null import NullWriter
from mabel.data import Writer, BatchWriter
from rich import traceback

traceback.install()

from mabel.data.writers.internals.blob_writer import BLOB_SIZE


def test_null_writer():
    # none of these should do anything
    nw = Writer(inner_writer=NullWriter, schema=["blob"], dataset="bucket/path")
    for i in range(1):
        nw.append({"blob": ["2"] * BLOB_SIZE})
    res = nw.finalize()
    assert res.startswith("NullWriter"), res


def test_hourly_partitions():
    nw = BatchWriter(
        inner_writer=NullWriter,
        dataset="bucket/path",
        schema=["@"],
        partitions=["year_{yyyy}/month_{mm}/day_{dd}/by_hour/hour={HH}"],
    )
    for i in range(1):
        nw.append({"@": [" "] * BLOB_SIZE})
    res = nw.finalize()
    assert "by_hour/hour=" in res


if __name__ == "__main__":  # pragma: no cover
    from tests.helpers.runner import run_tests

    run_tests()
