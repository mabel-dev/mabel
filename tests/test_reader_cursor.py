"""
Test the file reader
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.adapters.disk import DiskReader
from mabel.data import Reader
from rich import traceback

traceback.install()


def get_records():
    r = Reader(inner_reader=DiskReader, dataset="tests/data/tweets/", partitions=[])
    return len(list(r))


def test_cursor():

    test_counter = 0
    number_of_records = get_records()
    lim = number_of_records // 4 * 3

    reader = Reader(
        inner_reader=DiskReader, dataset="tests/data/tweets/", partitions=[]
    )

    for row in reader.take(lim):
        test_counter += 1
    cursor = reader.cursor

    print(cursor)
    assert cursor["location"] == ((lim - 1) % 25), f"{cursor['location']}, {lim}, {(lim % 25)}"
    assert cursor["partition"] == 5122091051124077700, cursor["partition"]

    reader = Reader(
        inner_reader=DiskReader,
        dataset="tests/data/tweets/",
        partitions=[],
        cursor=cursor,
    )

    for i, row in enumerate(reader):
        test_counter += 1

    assert number_of_records == test_counter, f"{number_of_records} - {test_counter}, {i}"


def test_cursor_as_text():

    offsets = [1, 6, 8, 13, 22]

    for offset in offsets:
        reader = Reader(
            inner_reader=DiskReader,
            dataset="tests/data/tweets/",
            partitions=[],
            cursor='{"partition": 5122091051124077700, "location": '
            + str(offset)
            + ', "map":"80" }',
        )
        reader = list(reader)
        assert len(reader) == 24 - offset, f"{len(reader)} == {24} - {offset}"


def test_base():

    reader = Reader(
        inner_reader=DiskReader,
        dataset="tests/data/tweets/",
        partitions=[]
    )

    for i in range(50):
        next(reader)
        assert reader.cursor["location"] == (i % 25), reader.cursor
    
    assert len(range(50)) == 50


if __name__ == "__main__":  # pragma: no cover
    test_cursor()
    test_cursor_as_text()
    test_base()

    print("okay")
