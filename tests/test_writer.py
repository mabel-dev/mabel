import shutil
import os
import sys
import glob

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.adapters.disk import DiskReader, DiskWriter
from mabel.adapters.null import NullWriter
from mabel.data import Reader, Writer
from rich import traceback

traceback.install()


def do_writer():
    w = Writer(inner_writer=DiskWriter, dataset="_temp")
    for i in range(int(1e5)):
        w.append({"Barney Stinson": "Lorenzo Von Matterhorn"})
        w.append({"Laszlo Cravensworth": "Jackie Daytona"})
        w.append({"name": "Pheobe Buffay", "alter": "Regina Phalange"})
    w.finalize()


def do_writer_compressed(algo):
    w = Writer(inner_writer=DiskWriter, dataset="_temp", format=algo)
    for i in range(int(1e5)):
        w.append({"test": True})
        w.append({"test": False})
    w.finalize()
    del w


def do_writer_default():
    w = Writer(inner_writer=DiskWriter, dataset="_temp")
    for i in range(int(1e5)):
        w.append({"Barney Stinson": "Lorenzo Von Matterhorn"})
        w.append({"Laszlo Cravensworth": "Jackie Daytona"})
    w.finalize()
    del w


def do_writer_csv():
    w = Writer(inner_writer=DiskWriter, dataset="_temp", format="text")
    w.append("week,volume")
    for i in range(int(100)):
        w.append(f"{i},{i}")
    w.finalize()


def test_reader_writer():
    do_writer()
    r = Reader(inner_reader=DiskReader, dataset="_temp")
    l = len(list(r))
    shutil.rmtree("_temp", ignore_errors=True)
    assert l == 300000, l


def test_reader_writer_format_zstd():
    do_writer_compressed("zstd")
    g = glob.glob("_temp/**/*.zstd", recursive=True)
    assert len(g) > 0, g
    r = Reader(inner_reader=DiskReader, dataset="_temp")
    l = len(list(r))
    shutil.rmtree("_temp", ignore_errors=True)
    assert l == 200000, l


def test_reader_writer_format_jsonl():
    do_writer_compressed("jsonl")
    g = glob.glob("_temp/**/*.jsonl", recursive=True)
    assert len(g) > 0, g

    r = Reader(inner_reader=DiskReader, dataset="_temp")
    l = len(list(r))
    shutil.rmtree("_temp", ignore_errors=True)
    assert l == 200000, l


def test_reader_writer_format_parquet():
    do_writer_compressed("parquet")
    g = glob.glob("_temp/**/*.parquet", recursive=True)
    assert len(g) > 0, g
    r = Reader(inner_reader=DiskReader, dataset="_temp")
    l = len(list(r))
    shutil.rmtree("_temp", ignore_errors=True)
    assert l == 200000, l


def test_reader_writer_format_text():
    do_writer_compressed("text")
    g = glob.glob("_temp/**/*.txt", recursive=True)
    assert len(g) > 0, g

    r = Reader(inner_reader=DiskReader, dataset="_temp")
    l = len(list(r))
    shutil.rmtree("_temp", ignore_errors=True)
    assert l == 1, l


def test_reader_writer_format_default():
    do_writer_default()
    g = glob.glob("_temp/**/*.zstd", recursive=True)
    assert len(g) > 0, g

    r = Reader(inner_reader=DiskReader, dataset="_temp")
    l = len(list(r))
    shutil.rmtree("_temp", ignore_errors=True)
    assert l == 200000, l


def test_write_to_path_logged():
    # none of these should do anything
    nw = Writer(inner_writer=NullWriter, to_path="bucket/path")
    nw.append({"abc": "def"})
    print(nw.finalize())


def get_data():
    r = Reader(inner_reader=DiskReader, dataset="tests/data/tweets", partitioning=[])
    return r


if __name__ == "__main__":  # pragma: no cover
    test_reader_writer()
    test_reader_writer_format_zstd()
    test_reader_writer_format_jsonl()
    test_reader_writer_format_parquet()
    test_reader_writer_format_default()
    test_reader_writer_format_text()
    test_write_to_path_logged()

    print("okay")
