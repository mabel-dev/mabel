import shutil
import datetime
import os
import sys
import glob

from pyarrow import parquet

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.adapters.disk import DiskReader, DiskWriter
from mabel.data import BatchWriter
from mabel.data import Reader
from mabel.data.internals.dictset import STORAGE_CLASS
from mabel.utils import entropy
from rich import traceback

traceback.install()


def do_writer():
    w = BatchWriter(
        inner_writer=DiskWriter,
        dataset="_temp",
        date=datetime.datetime.utcnow().date(),
        format="parquet",
        schema={
            "fields": [
                {"name": "name", "type": "VARCHAR"},
                {"name": "alias", "type": "VARCHAR"},
                {"name": "season", "type": "INTEGER"},
                {"name": "recurring", "type": "BOOLEAN"},
                {"name": "list_test", "type": "ARRAY"},
                {"name": "dict_test", "type": "STRUCT"},
            ]
        },
    )
    for i in range(int(5e5)):
        w.append(
            {
                "name": None,
                "alias": entropy.random_string(256),
                "season": None,
                "recurring": None,
                "list_test": [],
                "dict_test": {"one": "1"},
            }
        )
    for i in range(int(1e5)):
        w.append(
            {
                "name": "Barney Stinson",
                "alias": "Lorenzo Von Matterhorn",
                "season": 4,
                "recurring": False,
                "list_test": ["a"],
                "dict_test": {"a": "b"},
            }
        )
        w.append(
            {
                "name": "Laszlo Cravensworth",
                "alias": "Jackie Daytona",
                "season": 3,
                "recurring": True,
                "list_test": ["ai", "bee", "see", "dee", "eff", "ghee"],
                "dict_test": {"alpha": "bet", "a": "gamma"},
            }
        )
    w.finalize()


def test_reader_writer_parquet_normalization():
    do_writer()

    parquet_files = glob.glob("_temp/**/*.parquet", recursive=True)

    for parquet_file in parquet_files:
        table = parquet.read_table(parquet_file)
        print(table.schema.names, table.schema.types)

    assert len(parquet_files) > 0, parquet_files

    c = glob.glob("_temp/**/*.complete", recursive=True)
    len(c) == 0, c

    parq = Reader(inner_reader=DiskReader, dataset="_temp", persistence=STORAGE_CLASS.MEMORY)
    records = parq.count()
    recurring = parq.collect_list("recurring")
    aliases = [
        isinstance(a, datetime.datetime) for a in parq.collect_list("alias") if a is not None
    ]
    shutil.rmtree("_temp", ignore_errors=True)
    assert records == 700000, records
    assert recurring.count(True) == 100000, recurring.count(True)
    assert recurring.count(None) == 500000, recurring.count(None)
    assert all(a is not None for a in aliases), all(a is not None for a in aliases)


if __name__ == "__main__":  # pragma: no cover
    from tests.helpers.runner import run_tests

    run_tests()
