import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
import mabel
from rich import traceback

traceback.install()


def test_version():
    assert hasattr(mabel, "__version__")


if __name__ == "__main__":  # pragma: no cover
    from tests.helpers.runner import run_tests

    run_tests()
