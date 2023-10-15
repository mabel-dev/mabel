import time
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.adapters.disk import DiskWriter, DiskReader
from mabel.data import StreamWriter
from mabel.data import Reader
from mabel.data.validator import schema_loader
import shutil
from pathlib import Path
from rich import traceback

traceback.install()


DATA_SET = [
    {"key": 6},
    {"key": 10},
    {"key": 3},
    {"key": 9},
    {"key": "eight"},
    {"key": 4},
    {"key": 7},
    {"key": 5},
    {"key": "two"},
    {"key": 1},
]
SCHEMA = {"fields": [{"name": "key", "type": "INTEGER"}]}
TEST_FOLDER = "_temp/path"


def test_writer_backout():
    if Path(TEST_FOLDER).exists():  # pragma: no cover
        shutil.rmtree(TEST_FOLDER)

    w = StreamWriter(
        dataset=TEST_FOLDER,
        inner_writer=DiskWriter,
        schema=SCHEMA,
        idle_timeout_seconds=1,
    )

    for record in DATA_SET:
        w.append(record)

    time.sleep(4)

    r = Reader(dataset=TEST_FOLDER, inner_reader=DiskReader)

    assert len(list(r)) == 8


if __name__ == "__main__":  # pragma: no cover
    from tests.helpers.runner import run_tests

    run_tests()
