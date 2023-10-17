import shutil
import os
import sys
import glob

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.adapters.disk import DiskReader, DiskWriter
from mabel.adapters.null import NullWriter
from mabel.data import Reader, Writer


def do_writer():
    shutil.rmtree("_temp", ignore_errors=True)
    w = Writer(inner_writer=DiskWriter, dataset="_temp", schema=["name", "alter"])
    for i in range(int(1e5)):
        w.append({"name": "Barney Stinson", "alter": "Lorenzo Von Matterhorn"})
        w.append({"name": "Laszlo Cravensworth", "alter": "Jackie Daytona"})
        w.append({"name": "Pheobe Buffay", "alter": "Regina Phalange"})
    w.finalize()


def do_writer_compressed(algo):
    w = Writer(inner_writer=DiskWriter, dataset="_temp", format=algo, schema=["test"])
    for i in range(int(1e5)):
        w.append({"test": True})
        w.append({"test": False})
    w.finalize()
    del w


def do_writer_default():
    w = Writer(inner_writer=DiskWriter, dataset="_temp", schema=["name", "person"])
    for i in range(int(1e5)):
        w.append({"name": "Barney Stinson", "person": "Lorenzo Von Matterhorn"})
        w.append({"name": "Laszlo Cravensworth", "person": "Jackie Daytona"})
    w.finalize()
    del w


def do_writer_csv():
    w = Writer(inner_writer=DiskWriter, dataset="_temp", format="text", schema=[])
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
    nw = Writer(inner_writer=NullWriter, to_path="bucket/path", schema=["abc"])
    nw.append({"abc": "def"})
    print(nw.finalize())


def get_data():
    r = Reader(inner_reader=DiskReader, dataset="tests/data/tweets", raw_path=True)
    return r


if __name__ == "__main__":  # pragma: no cover
    from tests.helpers.runner import run_tests

    run_tests()
