"""
Written following a bug discovered in how the Reader determines the data range.
"""
import datetime
import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from mabel.data.readers.internals.base_inner_reader import BaseInnerReader
from rich import traceback

traceback.install()


class EmptyReader(BaseInnerReader):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
    def get_blobs_at_path(self): pass
    def get_blob_stream(self): pass


START_DATE = datetime.date(2000,1,1)
END_DATE   = datetime.date(2010,6,15)
TODAY      = datetime.date.today()

def test_start_date_only():
    subject = EmptyReader(dataset='', start_date=START_DATE)
    assert subject.start_date == START_DATE, subject.start_date
    assert subject.end_date == TODAY

def test_end_date_only():
    subject = EmptyReader(dataset='', end_date=END_DATE)
    assert subject.start_date == TODAY
    assert subject.end_date == END_DATE

def test_start_and_end_dates():
    subject = EmptyReader(dataset='', start_date=START_DATE, end_date=END_DATE)
    assert subject.start_date == START_DATE
    assert subject.end_date == END_DATE

def test_dates_as_string():
    subject = EmptyReader(dataset='', start_date='2000-01-01', end_date='2010-06-15')
    assert subject.start_date == START_DATE, subject.start_date
    assert subject.end_date == END_DATE   

if __name__ == "__main__":  # pragma: no cover
    test_start_date_only()
    test_end_date_only()
    test_start_and_end_dates()
    test_dates_as_string()

    print('okay')
    