import datetime
import pytest
import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from mabel.adapters.disk import DiskReader
from mabel.data import Reader
try:
    from rich import traceback
    traceback.install()
except ImportError:   # pragma: no cover
    pass


def test_can_read_parquet():
    r = Reader(
            inner_reader=DiskReader,
            row_format='pass-thru',
            dataset='tests/data/formats/parquet',
            raw_path=True)

    for i, row in enumerate(r):
        pass

    assert i == 57580
    assert isinstance(row, str)

if __name__ == "__main__":
    test_can_read_parquet()

    print('okay')
    