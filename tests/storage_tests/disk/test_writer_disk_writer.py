import shutil
import datetime
import os
import sys
import glob


sys.path.insert(1, os.path.join(sys.path[0], "../../.."))
from mabel.adapters.disk import DiskReader, DiskWriter
from mabel.adapters.null import NullWriter
from mabel.data import BatchWriter
from mabel.data import Reader
from mabel.data.internals.dictset import STORAGE_CLASS


def do_writer():
    w = BatchWriter(
        inner_writer=DiskWriter,
        dataset="_temp",
        date=datetime.datetime.utcnow().date(),
        schema=["character", "alter"],
    )
    for i in range(int(1e5)):
        w.append({"character": "Barney Stinson", "alter": "Lorenzo Von Matterhorn"})
        w.append({"character": "Laszlo Cravensworth", "alter": "Jackie Daytona"})
    w.finalize()


def do_writer_abs():
    w = BatchWriter(
        inner_writer=DiskWriter,
        dataset=os.getcwd() + "/_temp",
        date=datetime.datetime.utcnow().date(),
        schema=["character", "alter"],
    )
    for i in range(int(1e5)):
        w.append({"character": "Barney Stinson", "alter": "Lorenzo Von Matterhorn"})
        w.append({"character": "Laszlo Cravensworth", "alter": "Jackie Daytona"})
    w.finalize()


def do_writer_compressed(algo):
    print(algo)
    w = BatchWriter(
        inner_writer=DiskWriter,
        dataset="_temp",
        format=algo,
        schema=["test", "another", "list", "today", "final"],
    )
    for i in range(int(1e5)):
        w.append(
            {
                "test": True,
                "list": list(range(100)),
                "today": None,  # this must be none, it's part of a test
                # another is missing as part of a test
            }
        )
        w.append(
            {
                "test": False,
                "another": "1",
                "today": datetime.datetime.now(),
                "list": [1, 2, 3, 4, 5],
            }
        )
    w.append({"test": False, "final": True})
    w.finalize()
    del w


def do_writer_default():
    w = BatchWriter(inner_writer=DiskWriter, dataset="_temp", schema=["character", "alter"])
    for i in range(int(1e5)):
        w.append({"character": "Barney Stinson", "alter": "Lorenzo Von Matterhorn"})
        w.append({"character": "Laszlo Cravensworth", "alter": "Jackie Daytona"})
    w.finalize()
    del w


def test_reader_writer_abs():
    do_writer_abs()

    r = Reader(inner_reader=DiskReader, dataset="_temp")
    l = len(list(r))
    shutil.rmtree("_temp", ignore_errors=True)
    assert l == 200000, l


def test_reader_writer():
    do_writer()

    r = Reader(inner_reader=DiskReader, dataset="_temp")
    l = len(list(r))
    shutil.rmtree("_temp", ignore_errors=True)
    assert l == 200000, l


def test_reader_writer_format_zstd():
    do_writer_compressed("zstd")

    g = glob.glob("_temp/**/*.zstd", recursive=True)
    assert len(g) > 0, g

    c = glob.glob("_temp/**/*.complete", recursive=True)
    len(c) == 0, c

    r = Reader(inner_reader=DiskReader, dataset="_temp")
    l = len(list(r))
    shutil.rmtree("_temp", ignore_errors=True)
    assert l == 200001, l


def test_reader_writer_format_jsonl():
    do_writer_compressed("jsonl")

    g = glob.glob("_temp/**/*.jsonl", recursive=True)
    assert len(g) > 0, g

    c = glob.glob("_temp/**/*.complete", recursive=True)
    len(c) == 0, c

    r = Reader(inner_reader=DiskReader, dataset="_temp")
    l = len(list(r))
    shutil.rmtree("_temp", ignore_errors=True)
    assert l == 200001, l


def test_reader_writer_format_parquet():
    """
    Ensure missing values and columns are correctly handled, parquet is more sensitive
    to this as it will use the schema of the first row to set the entire table. Using
    orso as a WAL (a write buffer) we should collect a table at a time and convert to
    parquet - but let's be really sure of this.
    """
    do_writer_compressed("parquet")

    g = glob.glob("_temp/**/*.parquet", recursive=True)
    assert len(g) > 0, g

    c = glob.glob("_temp/**/*.complete", recursive=True)
    len(c) == 0, c

    print("written")
    parq = Reader(inner_reader=DiskReader, dataset="_temp", persistence=STORAGE_CLASS.MEMORY)

    records = parq.count()
    tests = parq.collect_list("test")
    dates = [isinstance(a, datetime.datetime) for a in parq.collect_list("today")]
    final = parq.collect_list("final")
    shutil.rmtree("_temp", ignore_errors=True)

    # do we have the number of records we're expecting?
    assert records == 200001, records
    # do we have the columns we're expecting?
    assert set(parq.keys()) == {"test", "another", "list", "today", "final"}, parq.keys()
    # is a populated column set correctly?
    assert tests.count(True) == 100000, tests.count(True)
    # if a column is null in the first row, is it written correctly?
    assert dates.count(True) == 100000, dates.count(True)
    # if a column is missing, is it written correctly?
    assert final.count(True) == 1, final.count(True)


#    from orso import DataFrame
#    df = DataFrame(parq._iterator.data)
#    print(df.tail(5))


def test_reader_writer_format_default():
    do_writer_default()

    g = glob.glob("_temp/**/*.zstd", recursive=True)
    assert len(g) > 0, g

    c = glob.glob("_temp/**/*.complete", recursive=True)
    len(c) == 0, c

    r = Reader(inner_reader=DiskReader, dataset="_temp")
    l = len(list(r))
    shutil.rmtree("_temp", ignore_errors=True)
    assert l == 200000, l


def get_data():
    r = Reader(inner_reader=DiskReader, dataset="tests/data/tweets", raw_path=True)
    return r


if __name__ == "__main__":  # pragma: no cover
    from tests.helpers.runner import run_tests

    run_tests()
