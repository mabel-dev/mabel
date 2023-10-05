import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel import Reader, DictSet
from mabel.data import STORAGE_CLASS
from mabel.data.internals.display import html_table
from mabel.adapters.disk import DiskReader
from orso.logging import get_logger

get_logger().setLevel(5)


def get_ds(**kwargs):
    ds = Reader(inner_reader=DiskReader, dataset="tests/data/tweets", raw_path=True, **kwargs)
    return ds


def test_html_table():
    ds = get_ds(persistence=STORAGE_CLASS.MEMORY)
    html = html_table(ds)

    assert "<table" in html
    assert "<th" in html
    assert "<tr" in html
    assert "</table>" in html


if __name__ == "__main__":  # pragma: no cover
    from tests.helpers.runner import run_tests

    run_tests()
