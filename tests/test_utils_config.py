import sys
import os
import pytest

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.utils import common
from rich import traceback

traceback.install()


def test_utils_config():
    config = {}
    with pytest.raises((IndexError)):
        common.build_context(config_file="not existent config file")

    with pytest.raises((ValueError)):
        common.build_context(config_file="tests/data/invalid.config")


if __name__ == "__main__":  # pragma: no cover
    from tests.helpers.runner import run_tests

    run_tests()
