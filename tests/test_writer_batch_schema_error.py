import shutil
import datetime
import os
import sys
import pytest

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.adapters.disk import DiskReader, DiskWriter
from mabel.adapters.null import NullWriter
from mabel.data import BatchWriter
from mabel.data import Reader
from orso.exceptions import DataValidationError, ExcessColumnsInDataError

dataset = {
    "dataset": "_temp",
    "partitions": ["year_{yyyy}"],
    "format": "parquet",
    "schema": [
        {"name": "character", "type": "VARCHAR", "description": "name"},
        {"name": "persona", "type": "VARCHAR", "description": "named"},
    ],
}


def test_new_schema():
    shutil.rmtree("_temp", ignore_errors=True)
    w = BatchWriter(inner_writer=DiskWriter, date=datetime.datetime.utcnow().date(), **dataset)
    with pytest.raises(ExcessColumnsInDataError) as err:
        w.append(
            {"character": "Barney Stinson", "persona": "Lorenzo Von Matterhorn", "height": 123}
        )
    assert "height" in str(err)

    with pytest.raises(DataValidationError) as err:
        w.append({"persona": "Jackie Daytona"})
    assert "character" in str(err)
    w.finalize()


def test_blobs():

    ds = {
        "dataset": "_temp",
        "partitions": ["year_{yyyy}"],
        "format": "parquet",
        "schema": [
            {"name": "character", "type": "VARCHAR", "description": "name"},
            {"name": "deets", "type": "BLOB", "description": "named"},
        ],
    }

    shutil.rmtree("_temp", ignore_errors=True)
    w = BatchWriter(inner_writer=NullWriter, date=datetime.datetime.utcnow().date(), **dataset)
    with pytest.raises(ExcessColumnsInDataError) as err:
        w.append({"character": "Barney Stinson", "deets": {"age": 32, "job": "blogger"}})

    with pytest.raises(DataValidationError) as err:
        w.append({"persona": "Jackie Daytona"})
    assert "character" in str(err)
    w.finalize()


if __name__ == "__main__":  # pragma: no cover
    from tests.helpers.runner import run_tests

    run_tests()
