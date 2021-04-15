"""
Written following a bug discovered in how the Reader determines the data range.
"""
import datetime
import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from mabel.data import Reader
from mabel.adapters.disk import DiskReader
try:
    from rich import traceback
    traceback.install()
except ImportError:   # pragma: no cover
    pass

DATA_DATE = datetime.date(2021,3,29)


def test_ignore_flag():
    records = Reader(
            dataset='tests/data/framed',
            inner_reader=DiskReader,
            start_date=DATA_DATE,
            end_date=DATA_DATE)
    print(next(records))
    assert next(records).get('test') == 1

if __name__ == "__main__":
    test_ignore_flag()

    print('okay')
    