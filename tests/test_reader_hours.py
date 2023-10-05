import datetime
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel import Reader
from mabel.adapters.disk import DiskReader
from rich import traceback

traceback.install()


START_DATE = datetime.datetime(2020, 12, 21, 18)
END_DATE = datetime.datetime(2020, 12, 22, 1)


def test_reader_times():
    subject = Reader(
        dataset="tests/data/dated",
        partitions=["year_{yyyy}/month_{mm}/day_{dd}/by_hour/hour={HH}"],
        start_date=START_DATE,
        end_date=END_DATE,
        inner_reader=DiskReader,
    )

    records = subject.collect_list()
    assert len(records) == 4, len(records)


if __name__ == "__main__":  # pragma: no cover
    from tests.helpers.runner import run_tests

    run_tests()
