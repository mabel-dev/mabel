import time
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.operators.reader_operator import ReaderOperator
from mabel.errors.time_exceeded import TimeExceeded
from mabel.adapters.disk import DiskReader
from mabel.data.formats import json
from rich import traceback

traceback.install()


from mabel.logging import get_logger

get_logger().setLevel(5)


def test_reader_timeout():
    reader = ReaderOperator(
        inner_reader=DiskReader, dataset="tests/data/tweets", raw_path=True, time_out=1
    )
    dataset = reader.execute(None, None)
    failed = False
    try:
        for i, record in enumerate(dataset):
            time.sleep(0.1)
    except TimeExceeded as te:
        cursor = json.parse(str(te))
        assert (i + 1) == cursor["offset"]
        failed = True
    assert failed


if __name__ == "__main__":  # pragma: no cover
    test_reader_timeout()

    print("okay")
