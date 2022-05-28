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
from rich import traceback

traceback.install()


def do_writer():
    w = BatchWriter(
        inner_writer=DiskWriter, dataset="_temp", date=datetime.date.today()
    )
    for i in range(int(1e5)):
        w.append({"Barney Stinson": "Lorenzo Von Matterhorn"})
        w.append({"Laszlo Cravensworth": "Jackie Daytona"})
    w.finalize()


def do_writer_abs():
    w = BatchWriter(
        inner_writer=DiskWriter,
        dataset=os.getcwd() + "/_temp",
        date=datetime.date.today(),
    )
    for i in range(int(1e5)):
        w.append({"Barney Stinson": "Lorenzo Von Matterhorn"})
        w.append({"Laszlo Cravensworth": "Jackie Daytona"})
    w.finalize()


def do_writer_compressed(algo):
    w = BatchWriter(inner_writer=DiskWriter, dataset="_temp", format=algo)
    for i in range(int(1e5)):
        w.append({"test": True})
        w.append({"test": False})
    w.append({"not_test": True})
    w.finalize()
    del w


def do_writer_default():
    w = BatchWriter(inner_writer=DiskWriter, dataset="_temp")
    for i in range(int(1e5)):
        w.append({"Barney Stinson": "Lorenzo Von Matterhorn"})
        w.append({"Laszlo Cravensworth": "Jackie Daytona"})
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

    do_writer_compressed("parquet")

    g = glob.glob("_temp/**/*.parquet", recursive=True)
    assert len(g) > 0, g

    c = glob.glob("_temp/**/*.complete", recursive=True)
    len(c) == 0, c

    r = Reader(inner_reader=DiskReader, dataset="_temp")
    l = len(list(r))
    shutil.rmtree("_temp", ignore_errors=True)
    assert l == 200001, l


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
    test_reader_writer()
    test_reader_writer_format_zstd()
    test_reader_writer_format_jsonl()
    test_reader_writer_format_parquet()
    test_reader_writer_format_default()
    test_reader_writer_abs()

    print("okay")
