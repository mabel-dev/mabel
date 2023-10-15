import datetime
import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.data import Reader
from mabel.adapters.disk import DiskReader
from rich import traceback
from mabel.data.internals.dictset import STORAGE_CLASS
from mabel.errors import DataNotFoundError

traceback.install()


def test_reader_partitions_read_without_referring_to_partition():
    """
    test if we reference a folder with partitions (by_) without referencing the
    partition, we pick a partition and read it like it's not there
    """
    DATA_DATE = datetime.date(2020, 2, 3)
    records = Reader(
        dataset="tests/data/partitioned",
        inner_reader=DiskReader,
        start_date=DATA_DATE,
        end_date=DATA_DATE,
        persistence=STORAGE_CLASS.MEMORY,
    )
    assert records.count() == 25, records.count()


def test_reader_partitions_read_without_referring_to_partition():
    """
    test if we reference a folder with partitions (by_) without referencing the
    partition, we pick a partition and read it like it's not there
    """
    DATA_DATE = datetime.date(2020, 2, 3)
    records = Reader(
        dataset="tests/data/partitioned",
        inner_reader=DiskReader,
        start_date=DATA_DATE,
        end_date=DATA_DATE,
        persistence=STORAGE_CLASS.MEMORY,
    )
    assert records.count() == 25, records.count()


def test_reader_partitions_read_referring_to_specific_partition():
    """
    test if we reference a folder with partitions (by_) without referencing the
    partition, we pick a partition and read it like it's not there
    """
    DATA_DATE = datetime.date(2020, 2, 3)
    records = Reader(
        dataset="tests/data/partitioned",
        partitions=["year_{yyyy}/month_{mm}/day_{dd}"],
        partition_filter=("userid", "=", "14173315"),
        inner_reader=DiskReader,
        start_date=DATA_DATE,
        end_date=DATA_DATE,
        persistence=STORAGE_CLASS.MEMORY,
    )
    assert records.count() == 5, records.count()

    DATA_DATE = datetime.date(2020, 2, 3)
    records = Reader(
        dataset="tests/data/partitioned",
        partitions=["year_{yyyy}/month_{mm}/day_{dd}"],
        partition_filter=("username", "=", "BBCNews"),
        inner_reader=DiskReader,
        start_date=DATA_DATE,
        end_date=DATA_DATE,
        persistence=STORAGE_CLASS.MEMORY,
    )
    assert records.count() == 4, records.count()

    with pytest.raises(DataNotFoundError):
        DATA_DATE = datetime.date(2020, 2, 3)
        records = Reader(
            dataset="tests/data/partitioned",
            partitions=["year_{yyyy}/month_{mm}/day_{dd}"],
            partition_filter=("username", "=", "CNNNews"),
            inner_reader=DiskReader,
            start_date=DATA_DATE,
            end_date=DATA_DATE,
            persistence=STORAGE_CLASS.MEMORY,
        )
        assert records.count() == 0, records.count()


if __name__ == "__main__":  # pragma: no cover
    from tests.helpers.runner import run_tests

    run_tests()
