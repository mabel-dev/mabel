import pytest
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))
from mabel.adapters.google import GoogleCloudStorageReader
from rich import traceback

traceback.install()


def test_blockers():
    # project is no longer required
    r = GoogleCloudStorageReader(dataset="path")

    # path is required
    with pytest.raises((ValueError, TypeError)):
        r = GoogleCloudStorageReader()


if __name__ == "__main__":  # pragma: no cover
    from tests.helpers.runner import run_tests

    run_tests()
