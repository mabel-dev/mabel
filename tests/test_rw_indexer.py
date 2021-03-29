import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from mabel.data import Reader, BatchWriter
from mabel.adapters.local import FileReader, FileWriter
try:
    from rich import traceback
    traceback.install()
except ImportError:   # pragma: no cover
    pass

from mabel.logging import get_logger
get_logger().setLevel(5)


def test_index():
    # step back through time
    r = Reader(
        inner_reader=FileReader,
        dataset='tests/data/tweets',
        raw_path=True)
    w = BatchWriter(
        inner_writer=FileWriter,
        dataset='_temp/data/tweets',
        index_on=['username']
    )
    for item in r:
        w.append(item)
    w.finalize()

if __name__ == "__main__":
    test_index()

    print('okay')
