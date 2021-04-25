import datetime
import pytest
import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from mabel.adapters.disk import DiskReader
from mabel.data.readers.internals.filters import Filters
from mabel import Reader
from rich import traceback

traceback.install()



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


def test_filters():

    TEST_DATA = [
        { "name": "Sirius Black", "age": 40, "dob": "1970-01-02", "gender": "male" },
        { "name": "Harry Potter", "age": 11, "dob": "1999-07-30", "gender": "male" },
        { "name": "Hermione Grainger", "age": 10, "dob": "1999-12-14", "gender": "female" },
        { "name": "Fleur Isabelle Delacour", "age": 11, "dob": "1999-02-08", "gender": "female" },
        { "name": "James Potter", "age": 40, "dob": "1971-12-30", "gender": "male" },
        { "name": "James Potter", "age": 0, "dob": "2010-12-30", "gender": "male" }
    ]

    d = Filters(filters=[('age', '==', 11), ('gender', 'in', ('a', 'b', 'male'))])
    assert len(list(d.filter_dictset(TEST_DATA))) == 1


def test_empty_filters():

    TEST_DATA = [
        { "name": "Sirius Black", "age": 40, "dob": "1970-01-02", "gender": "male" },
        { "name": "Harry Potter", "age": 11, "dob": "1999-07-30", "gender": "male" },
        { "name": "Hermione Grainger", "age": 10, "dob": "1999-12-14", "gender": "female" },
        { "name": "Fleur Isabelle Delacour", "age": 11, "dob": "1999-02-08", "gender": "female" },
        { "name": "James Potter", "age": 40, "dob": "1971-12-30", "gender": "male" },
        { "name": "James Potter", "age": 0, "dob": "2010-12-30", "gender": "male" }
    ]

    d = Filters(filters=None)
    assert len(list(d.filter_dictset(TEST_DATA))) == 6
    

def test_like_filters():

    TEST_DATA = [
        { "name": "Sirius Black", "age": 40, "dob": "1970-01-02", "gender": "male" },
        { "name": "Harry Potter", "age": 11, "dob": "1999-07-30", "gender": "male" },
        { "name": "Hermione Grainger", "age": 10, "dob": "1999-12-14", "gender": "female" },
        { "name": "Fleur Isabelle Delacour", "age": 11, "dob": "1999-02-08", "gender": "female" },
        { "name": "James Potter", "age": 40, "dob": "1971-12-30", "gender": "male" },
        { "name": "James Potter", "age": 0, "dob": "2010-12-30", "gender": "male" }
    ]

    d = Filters(filters=[('dob', 'like', '%-12-%')])
    assert len(list(d.filter_dictset(TEST_DATA))) == 3

if __name__ == "__main__":  # pragma: no cover
    test_reader_filters_no_filter()
    test_reader_filters_single_filter()
    test_reader_filters_multiple_filter()
    test_filters()
    test_empty_filters()
    test_like_filters()

    print('okay')
    