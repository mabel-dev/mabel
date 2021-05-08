"""
Test the file reader
"""
import datetime
import pytest
import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from mabel.adapters.disk import DiskReader
from mabel.data import Reader
from rich import traceback

traceback.install()


def test_threaded():

    r = Reader(
        thread_count=8,
        inner_reader=DiskReader,
        dataset='tests/data/tweets/',
        raw_path=True)

    records = [a for a in r]
    assert len(records) == 50



if __name__ == "__main__":  # pragma: no cover
    test_threaded()

    print('okay')
    