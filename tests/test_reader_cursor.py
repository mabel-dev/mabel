"""
Test the file reader
"""
from itertools import count
import os
import sys
import orjson
import pytest

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.data.internals.dictset import STORAGE_CLASS
from mabel.adapters.disk import DiskReader
from mabel.adapters.null import NullReader
from mabel.data import Reader
from mabel.utils import entropy
from rich import traceback

traceback.install()


def get_records():
    r = Reader(
        inner_reader=DiskReader, dataset="tests/data/formats/jsonl", partitions=None
    )
    return len(list(r))


def test_cursor():
    """
    We're going to test the cursor by doing math on the records, if we don't get the
    right result, it's because we have more or less records than expected.

    To make life easier, we're going to use a literal dataset.
    """

    number_of_records = 10

    data = []
    for i in range(number_of_records):
        data.append({"one": 1, "index": i})

    reader = Reader(inner_reader=NullReader, dataset="none", partitions=None, data=data)

    # create random offsets for testing - it's illogical to have a 0 cursor
    offsets = (entropy.random_range(1, number_of_records) for i in range(20))

    for offset in offsets:

        first_reader = Reader(
            inner_reader=NullReader,
            dataset="none",
            partitions=None,
            data=data,
        )

        # we've going to read to a position in the dataset
        counter = 0
        tracker = []
        for i in range(offset):
            record = next(first_reader)
            counter += record["one"]
            tracker.append(record["index"])

        # we should have read offset records
        assert offset == counter
        # the cursor is zero-based, take one because we've be counting natural numbers
        assert (first_reader.cursor["location"] + 1) == counter

        # we're now going to create a second reader, and give it the cursor from the
        # first
        second_reader = Reader(
            inner_reader=NullReader,
            dataset="none",
            partitions=None,
            data=data,
            cursor=first_reader.cursor,
        )

        # if we keep going, we should get all of the records
        for record in second_reader:
            counter += record["one"]
            tracker.append(record["index"])

        assert counter == number_of_records
        assert tracker == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9], tracker


def test_cursor_as_text():
    """
    Test that if we pass the cursor as text, it is parsed correctly
    """
    number_of_records = get_records()

    # create random offsets for testing
    offsets = (entropy.random_range(1, number_of_records - 1) for i in range(5))

    for offset in offsets:
        cursor = {"location": offset, "map": "00", "partition": 17668429320131150782}
        reader = Reader(
            inner_reader=DiskReader,
            dataset="tests/data/formats/jsonl",
            partitions=None,
            cursor=orjson.dumps(cursor).decode(),
        )

        records_left = len(list(reader))
        # the offset is zero-based, for example it says 10, but it's 11 records
        assert (
            offset + 1
        ) + records_left == number_of_records, (
            f"{offset + 1 + records_left} != {number_of_records}"
        )


def test_move_to_cursor():
    """
    Test when we move to a cursor position, that we read all of the records
    """
    number_of_records = get_records()

    # create random offsets for testing
    offsets = (entropy.random_range(1, number_of_records - 1) for i in range(5))

    for offset in offsets:
        reader = Reader(
            inner_reader=DiskReader,
            dataset="tests/data/formats/jsonl",
            partitions=None,
            cursor={"location": offset, "map": "00", "partition": 17668429320131150782},
            persistence=STORAGE_CLASS.NO_PERSISTANCE,
        )

        assert (
            offset == reader.cursor["location"]
        ), f"{offset}, {reader.cursor['location']}"

        records_left = len(list(reader))
        # the offset is zero-based, for example it says 10, but it's 11 records
        assert (
            offset + 1
        ) + records_left == number_of_records, (
            f"{offset + 1 + records_left} != {number_of_records}"
        )


def test_zero_based_cursor():
    """
    Test that the cursor is actually zero-based
    """
    reader = Reader(
        inner_reader=DiskReader, dataset="tests/data/formats/jsonl", partitions=[]
    )
    number_of_records = get_records()

    # we read all of the records, one-by-one
    for i in range(number_of_records):
        next(reader)
        assert reader.cursor["location"] == i, reader.cursor

    # we can't read past the end
    with pytest.raises(StopIteration):
        next(reader)

    # test our assumptions about the 'range' function
    assert len(range(number_of_records)) == number_of_records
    assert list(range(5)) == [0, 1, 2, 3, 4]


if __name__ == "__main__":  # pragma: no cover
    test_cursor()
    test_move_to_cursor()
    test_cursor_as_text()
    test_zero_based_cursor()

    print("okay")
