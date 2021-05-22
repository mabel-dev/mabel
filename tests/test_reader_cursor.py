"""
Test the file reader
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.adapters.disk import DiskReader
from mabel.data import Reader
from mabel.data.formats.dictset import limit
from rich import traceback

traceback.install()

def get_records():
    r = Reader(
            inner_reader=DiskReader,
            dataset="tests/data/tweets/",
            raw_path=True)
    return len(list(r))


def test_cursor():

    test_counter = 0
    number_of_records = get_records()
    lim = number_of_records // 4 * 3

    reader = Reader(
            inner_reader=DiskReader,
            dataset="tests/data/tweets/",
            raw_path=True)

    for row in limit(reader, lim):
        test_counter += 1
    cursor = reader.cursor

    reader = Reader(
            inner_reader=DiskReader,
            dataset="tests/data/tweets/",
            raw_path=True,
            cursor=cursor)
    
    for i, row in enumerate(reader):
        test_counter += 1

    assert number_of_records == test_counter


if __name__ == "__main__":  # pragma: no cover
    test_cursor()

    print("okay")
