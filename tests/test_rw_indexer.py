import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from mabel.data import BatchWriter
from mabel.data import Reader
from mabel.adapters.disk import DiskReader, DiskWriter
from rich import traceback

traceback.install()


def test_index():
    # step back through time
    r = Reader(
        inner_reader=DiskReader,
        dataset='tests/data/tweets',
        raw_path=True)
    w = BatchWriter(
        inner_writer=DiskWriter,
        dataset='_temp/data/tweets',
        index_on=['username']
    )
    for item in r:
        w.append(item)
    w.finalize()

if __name__ == "__main__":  # pragma: no cover
    test_index()

    print('okay')
