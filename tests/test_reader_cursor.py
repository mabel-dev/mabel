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
    r = Reader(inner_reader=DiskReader, dataset="tests/data/tweets/", raw_path=True)
    return len(list(r))


def test_cursor():

    test_counter = 0
    number_of_records = get_records()
    lim = number_of_records // 4 * 3

    reader = Reader(
        inner_reader=DiskReader, dataset="tests/data/tweets/", raw_path=True
    )

    for row in reader.take(lim):
        test_counter += 1
    cursor = reader.cursor()

    assert isinstance(cursor, dict)

    print(cursor)
    assert cursor["offset"] == (lim % 25), cursor["offset"]
    assert cursor["partition"] == 6018338569497712008, cursor["partition"]

    reader = Reader(
        inner_reader=DiskReader,
        dataset="tests/data/tweets/",
        raw_path=True,
        cursor=cursor,
    )

    for i, row in enumerate(reader):
        test_counter += 1

    assert number_of_records == test_counter


def test_cursor_as_text():

    offsets = [1, 6, 8, 13, 22]

    for offset in offsets:
        reader = Reader(
            inner_reader=DiskReader,
            dataset="tests/data/tweets/",
            raw_path=True,
            cursor='{"partition": 6018338569497712008, "offset": ' + str(offset) + " }",
        )
        reader = list(reader)
        assert len(reader) == 25 - offset


if __name__ == "__main__":  # pragma: no cover
    test_cursor()
    test_cursor_as_text()

    print("okay")
