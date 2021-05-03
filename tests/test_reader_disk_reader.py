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


def test_can_find_files():
    """
    ensure we can find the test files
    the test folder has two files in it
    """
    # with a trailing /
    r = DiskReader(dataset='tests/data/tweets/', raw_path=True)
    assert len(list(r.get_list_of_blobs())) == 4  # 2 data, 2 index

    # without a trailing /
    r = DiskReader(dataset='tests/data/tweets', raw_path=True)
    assert len(list(r.get_list_of_blobs())) == 4  # 2 data, 2 index


def test_can_read_files():
    """ ensure we can read the test files """
    r = DiskReader(dataset='tests/data/tweets/', raw_path=True)
    for file in [b for b in r.get_list_of_blobs() if '/_SYS.' not in b]:
        for index, item in enumerate(r.get_records(file)):
            pass
        assert index == 24, index


def test_only_read_selected_rows():
    """ ensure we can read the test files """
    r = DiskReader(dataset='tests/data/tweets/', raw_path=True)
    for file in [b for b in r.get_list_of_blobs() if '/_SYS.' not in b]:
        for index, item in enumerate(r.get_records(file, rows=[1,2,3])):
            pass
        assert index == 2, index


def test_step_back():
    # step back through time
    r = Reader(
            inner_reader=DiskReader,
            dataset='tests/data/dated/{date}',
            start_date=datetime.date(2021,1,1),
            end_date=datetime.date(2021,1,1),
            step_back_days=30)
    assert len(list(r)) == 50

def test_step_past():
    # step back through time
    r = Reader(
            inner_reader=DiskReader,
            dataset='tests/data/dated/{date}',
            start_date=datetime.date(2021,1,1),
            end_date=datetime.date(2021,1,1),
            step_back_days=5)
    with pytest.raises(SystemExit):
        assert len(list(r)) == 0

if __name__ == "__main__":  # pragma: no cover
    test_can_find_files()
    test_can_read_files()
    test_step_back()
    test_step_past()
    test_only_read_selected_rows()

    print('okay')
    