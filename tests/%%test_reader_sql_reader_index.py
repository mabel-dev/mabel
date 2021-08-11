import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.adapters.disk import DiskReader
from mabel.data.readers.internals.sql_reader import SqlReader
from rich import traceback

traceback.install()


def test_sql_uses_index(caplog):

    s = SqlReader(
        "SELECT * FROM tests.data.index.is WHERE user_name='Verizon Support'",
        inner_reader=DiskReader,
        raw_path=True,
    )
    findings = list(s)
    assert len(findings) == 2, len(findings)

    if caplog:
        has_index_log = any(
            "Reading index from" in d for n, l, d in caplog.record_tuples
        )
        assert has_index_log
    else:
        print("This test must be run in pytest")


if __name__ == "__main__":  # pragma: no cover
    test_sql_uses_index(None)

    print("okay")
