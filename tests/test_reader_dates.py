"""
Written following a bug discovered in how the Reader determines the data range.
"""

import datetime
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.data.readers.internals.base_inner_reader import BaseInnerReader
from rich import traceback
from mabel import Reader
from mabel.adapters.disk import DiskReader

traceback.install()


class EmptyReader(BaseInnerReader):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def get_blobs_at_path(self):
        pass

    def get_blob_chunk(self):
        pass

    def get_blob_bytes(self):
        pass


START_DATE = datetime.datetime(2000, 1, 1)
END_DATE = datetime.datetime(2010, 6, 15)
TODAY = datetime.datetime.utcnow().replace(minute=0, second=0, microsecond=0)


def test_start_date_only():
    subject = EmptyReader(dataset="", start_date=START_DATE)
    assert subject.start_date == START_DATE, subject.start_date
    assert subject.end_date == TODAY, subject.end_date


def test_end_date_only():
    subject = EmptyReader(dataset="", end_date=END_DATE)
    # the values are reversed to make a valid range of start < end
    assert subject.start_date == END_DATE, subject.start_date
    assert subject.end_date == TODAY, subject.end_date


def test_start_and_end_dates():
    subject = EmptyReader(dataset="", start_date=START_DATE, end_date=END_DATE)
    assert subject.start_date == START_DATE
    assert subject.end_date == END_DATE


def test_dates_as_string():
    subject = EmptyReader(dataset="", start_date="2000-01-01", end_date="2010-06-15")
    assert subject.start_date == START_DATE, subject.start_date
    assert subject.end_date == END_DATE


def test_reading_across_dates():
    subject = Reader(
        inner_reader=DiskReader,
        dataset="tests/data/dated",
        start_date="2020-02-03",
        end_date="2020-02-04",
    )
    record_count = len(list(subject))
    assert record_count == 50, record_count


def test_short_dates():
    subject = Reader(
        inner_reader=DiskReader,
        dataset="tests/data/dated",
        partitions=["{yyyy}/{mm}/{dd}"],
        start_date="1979-08-23",
        end_date="1979-08-23",
    )
    assert len(list(subject)) == 25


if __name__ == "__main__":  # pragma: no cover
    from tests.helpers.runner import run_tests

    run_tests()
