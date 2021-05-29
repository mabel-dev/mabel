import os
import io
import sys
import glob
import shutil

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.data import BatchWriter
from mabel.data import Reader
from mabel.index.index import Index
from mabel.adapters.disk import DiskReader, DiskWriter
from rich import traceback


traceback.install()


def test_index():
    # step back through time
    shutil.rmtree('_temp/data/tweets', ignore_errors=True)

    r = Reader(inner_reader=DiskReader, dataset="tests/data/tweets", raw_path=True)
    w = BatchWriter(
        inner_writer=DiskWriter, dataset="_temp/data/tweets", index_on=["username"]
    )
    for item in r:
        w.append(item)
    w.finalize()
    index = glob.glob("_temp/data/tweets/**/*username.index", recursive=True)
    assert len(index) == 1, index

    # test the recently created index outside the reader
    i = Index(io.BytesIO(open(index[0], 'rb').read()))
    assert i.size == 50
    assert i.search("SwiftOnSecurity") == set()
    assert i.search("BBCNews") == {1, 2, 4, 44, 24, 25}

    # test the filter with an index
    ri = Reader(inner_reader=DiskReader, dataset="_temp/data/tweets", filters=('username','==','BBCNews'))
    ri = list(ri)

    assert len(ri) == 6



if __name__ == "__main__":  # pragma: no cover
    test_index()

    print("okay")
