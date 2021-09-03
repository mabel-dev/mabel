import datetime
import pytest
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))
from mabel.adapters.google import GoogleCloudStorageReader
from rich import traceback

traceback.install()


def test_blockers():

    # project is required
    with pytest.raises((ValueError, TypeError)):
        r = GoogleCloudStorageReader(dataset="path")

    # path is required
    with pytest.raises((ValueError, TypeError)):
        r = GoogleCloudStorageReader(project="project")


if __name__ == "__main__":  # pragma: no cover
    test_blockers()

    print("okay")
