"""
Test the file reader
"""
import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.adapters.disk import DiskReader
from mabel.data import Reader
from rich import traceback

traceback.install()


def get_records():
    r = Reader(inner_reader=DiskReader, dataset="tests/data/tweets/", partitions=[])
    return len(list(r))


def test_cursor():
    """
    Test that when we break a read in two, we read the right amount of records.
    """

    import json

    test_counter = 0
    number_of_records = get_records()
    lim = number_of_records // 4 * 3
    hashes = []

    reader = Reader(
        inner_reader=DiskReader, dataset="tests/data/tweets/", partitions=[]
    )

    for row in reader["tweet"].take(lim):
        hashes.append(hash(json.dumps(row)))
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

    for i, row in enumerate(reader["tweet"].take(100)):
        hashes.append(hash(json.dumps(row)))
        test_counter += 1

    # we should have read the number of expected records
    assert number_of_records == test_counter, f"{number_of_records} - {test_counter}, {i}"
    # we shouldn't have captured any duplicates
    assert len(hashes) == len(set(hashes)), f"{len(hashes)} == {len(set(hashes))}"


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
        l = list(reader)
        # 24 because we count from zero (the first row in the file is the 0th record)
        assert len(l) + offset == 24, f"{len(l) + offset} == {24}, {reader.cursor}"

def test_move_to_cursor():

    offsets = [1, 6, 8, 13, 22]

    for offset in offsets:
        reader = Reader(
            inner_reader=DiskReader,
            dataset="tests/data/tweets/",
            partitions=[]
        )
        next(reader)
        steps = 1
        while reader.cursor["location"] < offset:
            steps += 1
            next(reader)

        assert offset == reader.cursor["location"]
        l = len(list(reader))
        # we stepped offset number of records and then read l more, this should be 50
        assert steps + l == 50

def test_base():

    reader = Reader(
        inner_reader=DiskReader,
        dataset="tests/data/tweets/",
        partitions=[]
    )

    # we read 50 records
    for i in range(50):
        next(reader)
        assert reader.cursor["location"] == (i % 25), reader.cursor

    # we can't read 51
    with pytest.raises(StopIteration):
        next(reader)
    
    # range 50 actually is 50
    assert len(range(50)) == 50


if __name__ == "__main__":  # pragma: no cover
    test_cursor()
    test_move_to_cursor()
    test_cursor_as_text()
    test_base()

    print("okay")
