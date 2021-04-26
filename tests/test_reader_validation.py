"""
Test the parameter validation on the mabel.data.reader are working
"""
import datetime
import pytest
import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from mabel import Reader
from rich import traceback

traceback.install()


def test_reader_all_good():
    failed = False

    try:
        reader = Reader(
                project='',
                select=['a', 'b'],
                dataset='',
                date_range=(datetime.datetime.now(), datetime.datetime.now()),
                row_format='json')
    except TypeError:
        failed = True

    assert not failed
    

def test_reader_select_not_list():
    with pytest.raises( (TypeError) ):
        reader = Reader(
                project='',
                select='everything',
                dataset='',
                date_range=(datetime.datetime.now(), datetime.datetime.now()),
                row_format='json')


def test_reader_where_not_callable():
    with pytest.raises( (TypeError) ):
        reader = Reader(
                project='',
                select=['a', 'b'],
                dataset='',
                where=True,
                date_range=(datetime.datetime.now(), datetime.datetime.now()),
                row_format='json')



def test_format_not_known():
    with pytest.raises( (TypeError) ):
        reader = Reader(
                project='',
                select=['a', 'b'],
                dataset='',
                date_range=datetime.datetime.now(),
                row_format='excel')


if __name__ == "__main__":  # pragma: no cover
    test_reader_all_good()
    test_reader_select_not_list()
    test_reader_where_not_callable()
    test_format_not_known()