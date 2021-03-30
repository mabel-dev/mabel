import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
import mabel
try:
    from rich import traceback
    traceback.install()
except ImportError:
    pass

from mabel.logging import get_logger
get_logger().setLevel(5)


def test_version():
    assert hasattr(mabel, '__version__')

if __name__ == "__main__":
    test_version()

    print('okay')