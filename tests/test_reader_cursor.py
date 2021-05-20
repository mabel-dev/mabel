"""
Test the file reader
"""
import datetime
import pytest
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.adapters.disk import DiskReader
from mabel.data import Reader
from rich import traceback

traceback.install()


def test_cursor():
    r = Reader(
            inner_reader=DiskReader,
            dataset="tests/data/tweets/",
            raw_path=True,
            cursor={'blob': 'tests/data/tweets/tweets-0001.jsonl', 'offset': 20})
    
    assert len(list(r)) == 5


if __name__ == "__main__":  # pragma: no cover
    test_cursor()

    print("okay")
