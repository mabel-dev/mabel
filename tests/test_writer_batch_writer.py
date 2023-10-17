import shutil
import datetime
import os
import sys
import glob
import pytest

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.adapters.disk import DiskReader, DiskWriter
from mabel.data import BatchWriter
from mabel.data import Reader


def do_writer():
    w = BatchWriter(
        inner_writer=DiskWriter,
        dataset="_temp",
        date=datetime.datetime.utcnow().date(),
        schema=[{"name": "character", "type": "VARCHAR"}, {"name": "persona", "type": "VARCHAR"}],
    )
    for i in range(int(1e5)):
        w.append({"character": "Barney Stinson", "persona": "Lorenzo Von Matterhorn"})
        w.append({"character": "Laszlo Cravensworth", "persona": "Jackie Daytona"})
    w.finalize()


def test_writer_without_schema_jsonl():
    w = BatchWriter(
        inner_writer=DiskWriter,
        dataset="_temp",
        format="jsonl",
        date=datetime.datetime.utcnow().date(),
        schema=False,
    )
    for i in range(int(1e5)):
        w.append({"character": "Barney Stinson", "persona": "Lorenzo Von Matterhorn"})
        w.append({"name": "Laszlo Cravensworth", "persona": "Jackie Daytona"})
    w.finalize()


def test_writer_without_schema_parquet():
    with pytest.raises(ValueError):
        w = BatchWriter(
            inner_writer=DiskWriter,
            dataset="_temp",
            format="parquet",
            date=datetime.datetime.utcnow().date(),
            schema=False,
        )


def test_writer_without_schema_zstd():
    w = BatchWriter(
        inner_writer=DiskWriter,
        dataset="_temp",
        format="zstd",
        date=datetime.datetime.utcnow().date(),
        schema=False,
    )
    for i in range(int(1e5)):
        w.append({"character": "Barney Stinson", "persona": "Lorenzo Von Matterhorn"})
        w.append({"character": "Laszlo Cravensworth", "persona": "Jackie Daytona"})
    w.finalize()


def do_writer_compressed(algo):
    w = BatchWriter(
        inner_writer=DiskWriter,
        dataset="_temp",
        format=algo,
        schema=[{"name": "test", "type": "BOOLEAN"}],
    )
    for i in range(int(1e5)):
        w.append({"test": True})
        w.append({"test": False})
    w.finalize()
    del w


def do_writer_default():
    w = BatchWriter(
        inner_writer=DiskWriter,
        dataset="_temp",
        schema=[{"name": "character", "type": "VARCHAR"}, {"name": "persona", "type": "VARCHAR"}],
    )
    for i in range(int(1e5)):
        w.append({"character": "Barney Stinson", "persona": "Lorenzo Von Matterhorn"})
        w.append({"character": "Laszlo Cravensworth", "persona": "Jackie Daytona"})
    w.finalize()
    del w


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
    assert l == 200000, l


def test_reader_writer_format_jsonl():
    do_writer_compressed("jsonl")

    g = glob.glob("_temp/**/*.jsonl", recursive=True)
    assert len(g) > 0, g

    c = glob.glob("_temp/**/*.complete", recursive=True)
    len(c) == 0, c

    r = Reader(inner_reader=DiskReader, dataset="_temp")
    l = len(list(r))
    shutil.rmtree("_temp", ignore_errors=True)
    assert l == 200000, l


def test_reader_writer_format_parquet():
    do_writer_compressed("parquet")

    g = glob.glob("_temp/**/*.parquet", recursive=True)
    assert len(g) > 0, g

    c = glob.glob("_temp/**/*.complete", recursive=True)
    len(c) == 0, c

    r = Reader(inner_reader=DiskReader, dataset="_temp")
    l = len(list(r))
    shutil.rmtree("_temp", ignore_errors=True)
    assert l == 200000, l


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

    test_reader_writer()
    run_tests()
