"""

"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.utils.dates import date_range
import datetime
from rich import traceback

traceback.install()


def test_date_range():
    start = datetime.datetime(2000, 1, 1)
    end = datetime.datetime(2000, 12, 31)
    drange = list(date_range(start, end))

    # it's not all of the last day, 2000 is a leap year so is 365 (366 - 1)
    assert len(drange) == 365 * 24 + 1, len(drange)


if __name__ == "__main__":  # pragma: no cover
    from tests.helpers.runner import run_tests

    run_tests()
