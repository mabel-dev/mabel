# file deepcode ignore ReplaceAPI: test file only
"""
Test the parameter validation on the mabel.data.reader are working
"""
import datetime
import pytest
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.data import Reader
from rich import traceback
from mabel.errors import InvalidReaderConfigError

traceback.install()


def test_reader_all_good():
    failed = False

    try:
        reader = Reader(
            project="",
            select="a, b",
            dataset="",
            start_date=datetime.datetime.now(),
            end_date=datetime.datetime.now(),
        )
    except InvalidReaderConfigError:
        failed = True

    assert not failed


def test_dataset_prefix_validator():

    with pytest.raises(ValueError):
        reader = Reader(dataset="dataset", valid_dataset_prefixes=["drive/"])

    with pytest.raises(ValueError):
        reader = Reader(
            dataset="dataset", valid_dataset_prefixes=["list", "of", "items"]
        )

    # no whitelist - allow all
    reader = Reader(project="", dataset="dataset")

    # a list of one
    reader = Reader(project="", dataset="dataset", valid_dataset_prefixes=["dataset"])

    # a list of many
    reader = Reader(
        project="",
        dataset="dataset",
        valid_dataset_prefixes=["on", "the", "list", "dataset"],
    )


if __name__ == "__main__":  # pragma: no cover
    test_reader_all_good()
    test_dataset_prefix_validator()
