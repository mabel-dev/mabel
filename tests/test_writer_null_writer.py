import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from mabel.adapters.null import NullWriter
try:
    from rich import traceback
    traceback.install()
except ImportError:
    pass


def test_null_writer():

    # none of these should do anything
    nw = NullWriter(dataset='bucket/path/file.extension')
    assert nw.commit(None, None) == 'NullWriter'

if __name__ == "__main__":
    test_null_writer()

    print('okay')