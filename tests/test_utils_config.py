import datetime
import sys
import os
import pytest
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from mabel.utils import common
try:
    from rich import traceback
    traceback.install()
except ImportError:   # pragma: no cover
    pass


def test_utils_config():

    config = {}
    with pytest.raises( (IndexError) ):
        common.build_context(config_file='not existent config file')

    with pytest.raises( (ValueError) ):
        common.build_context(config_file='tests/data/invalid.config')


if __name__ == "__main__":
    test_utils_config()

    print('okay')
