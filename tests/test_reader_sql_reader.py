import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.data import Reader
from mabel.data import BatchWriter
from mabel.adapters.disk import DiskWriter, DiskReader
from mabel.data.readers.internals.alpha_sql_reader import SqlReader
import shutil
from rich import traceback

traceback.install()


def do_writer():
    w = BatchWriter(inner_writer=DiskWriter, dataset="_temp/twitter", raw_path=True)
    r = Reader(inner_reader=DiskReader, dataset="tests/data/tweets", raw_path=True)
    for tweet in r:
        w.append(tweet)
    w.finalize()


def test_most_basic_sql():

    shutil.rmtree("_temp", ignore_errors=True)
    do_writer()
    s = SqlReader("SELECT * FROM _temp.twitter", inner_reader=DiskReader, raw_path=True)
    findings = list(s)
    assert len(findings) == 50, len(findings)
    shutil.rmtree("_temp", ignore_errors=True)


def test_sql():

    shutil.rmtree("_temp", ignore_errors=True)
    do_writer()
    s = SqlReader(
        "SELECT username FROM _temp.twitter WHERE sentiment != 0",
        inner_reader=DiskReader,
        raw_path=True,
    )
    findings = list(s)
    assert len(findings) == 39, len(findings)
    shutil.rmtree("_temp", ignore_errors=True)


if __name__ == "__main__":  # pragma: no cover
    test_sql()
    test_most_basic_sql()

    print("okay")
