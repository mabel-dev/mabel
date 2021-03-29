"""

"""
import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from mabel.utils.common import date_range
try:
    from rich import traceback
    traceback.install()
except ImportError:
    pass
import datetime


def test_date_range():
    start = datetime.datetime(2000, 1, 1)
    end = datetime.datetime(2000, 12, 31)
    drange = list(date_range(start, end))

    # 2000 is a leap year
    assert len(drange) == 366



if __name__ == "__main__":
    test_date_range()


    print('okay')