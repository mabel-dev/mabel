import os
import sys
import pytest

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.adapters.disk import DiskReader, DiskWriter
from mabel.adapters.null import NullWriter
from mabel.data import Reader, Writer
from rich import traceback
from data_expectations import Expectations
from data_expectations.errors import ExpectationNotMetError

traceback.install()


VALID_TARGET = {
    "dataset": "_temp",
    "set_of_expectations": [
        {"expectation": "expect_column_to_exist", "column": "name"},
        {"expectation": "expect_column_to_exist", "column": "alter"},
    ],
}

MISSING_COLUMN = {
    "dataset": "_temp",
    "set_of_expectations": [{"expectation": "expect_column_to_exist", "column": "show"}],
}

INVALID_VALUE = {
    "dataset": "_temp",
    "set_of_expectations": [
        {
            "expectation": "expect_column_values_to_be_in_set",
            "column": "name",
            "symbols": ["Laszlo Cravensworth"],
        }
    ],
}


def test_validator_expected_to_work():
    w = Writer(inner_writer=NullWriter, schema=["name", "alter"], **VALID_TARGET)
    w.append({"name": "Barney Stinson", "alter": "Lorenzo Von Matterhorn"})
    w.append({"name": "Laszlo Cravensworth", "alter": "Jackie Daytona"})
    w.append({"name": "Pheobe Buffay", "alter": "Regina Phalange"})
    w.finalize()


def test_validator_expected_to_not_work():
    w = Writer(inner_writer=NullWriter, schema=["name", "alter"], **INVALID_VALUE)
    with pytest.raises(ExpectationNotMetError):
        w.append({"name": "Barney Stinson", "alter": "Lorenzo Von Matterhorn"})
    w.append({"name": "Laszlo Cravensworth", "alter": "Jackie Daytona"})

    w = Writer(inner_writer=NullWriter, schema=["name", "alter"], **MISSING_COLUMN)
    with pytest.raises(ExpectationNotMetError):
        w.append({"name": "Barney Stinson", "alter": "Lorenzo Von Matterhorn"})
    with pytest.raises(ExpectationNotMetError):
        w.append({"name": "Laszlo Cravensworth", "alter": "Jackie Daytona"})
    w.finalize()


if __name__ == "__main__":  # pragma: no cover
    from tests.helpers.runner import run_tests

    run_tests()
