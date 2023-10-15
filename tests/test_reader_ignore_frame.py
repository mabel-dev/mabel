# file deepcode ignore unguarded~next~call/test: test scripts only
import datetime
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.data import Reader
from mabel.adapters.disk import DiskReader
from rich import traceback

traceback.install()


def test_ignore_flag():
    """
    test we ignore invalidated frames
    """
    DATA_DATE = datetime.date(2021, 3, 29)
    records = Reader(
        dataset="tests/data/framed",
        inner_reader=DiskReader,
        start_date=DATA_DATE,
        end_date=DATA_DATE,
    )
    print(next(records))
    assert next(records).get("test") == 1


def test_ignore_flag_step_back_days():
    """
    test that we step back a day if all of the frames have been invalidated
    """
    DATA_DATE = datetime.date(2021, 3, 30)
    records = Reader(
        dataset="tests/data/framed",
        inner_reader=DiskReader,
        start_date=DATA_DATE,
        end_date=DATA_DATE,
        freshness_limit="24h",
    )
    print(next(records))


if __name__ == "__main__":  # pragma: no cover
    from tests.helpers.runner import run_tests

    run_tests()
