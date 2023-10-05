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
from mabel.utils import parameter_validator
from mabel.errors import InvalidReaderConfigError
from mabel.data.readers.reader import AccessDenied

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
    with pytest.raises(AccessDenied):
        reader = Reader(dataset="dataset", valid_dataset_prefixes=["drive/"])

    with pytest.raises(AccessDenied):
        reader = Reader(dataset="dataset", valid_dataset_prefixes=["list", "of", "items"])

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


def test_levenshtein():
    ld = parameter_validator.get_levenshtein_distance("abc", "def")
    assert ld == 3, ld

    ld = parameter_validator.get_levenshtein_distance("apples", "pear")
    assert ld == 5, ld

    ld = parameter_validator.get_levenshtein_distance("axe", "ace")
    assert ld == 1, ld


if __name__ == "__main__":  # pragma: no cover
    from tests.helpers.runner import run_tests

    run_tests()
