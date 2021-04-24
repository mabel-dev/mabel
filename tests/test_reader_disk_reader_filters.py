import datetime
import pytest
import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from mabel.adapters.disk import DiskReader
from mabel import Reader
try:
    from rich import traceback
    traceback.install()
except ImportError:   # pragma: no cover
    pass


def test_reader_filters_no_filter():
    """ ensure the reader filter is working as expected """
    r = Reader(
            inner_reader=DiskReader,
            dataset='tests/data/tweets/',
            raw_path=True,
            filters=[])
    for index, item in enumerate(r):
        pass
    assert index == 49, index

def test_reader_filters_single_filter():
    """ ensure the reader filter is working as expected """
    r = Reader(
            inner_reader=DiskReader,
            dataset='tests/data/tweets/',
            raw_path=True,
            filters=[('username', '==', 'NBCNews')])
    
    for index, item in enumerate(r):
        pass
    assert index == 43, index

def test_reader_filters_multiple_filter():
    """ ensure the reader filter is working as expected """
    r = Reader(
            inner_reader=DiskReader,
            dataset='tests/data/tweets/',
            raw_path=True,
            filters=[('username', '==', 'NBCNews'), ('timestamp', '>=', '2020-01-12T07:11:04')])
    for index, item in enumerate(r):
        pass
    assert index == 33, index




if __name__ == "__main__":
    test_reader_filters_no_filter()
    test_reader_filters_single_filter()
    test_reader_filters_multiple_filter()

    print('okay')
    