import datetime
import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from mabel.adapters.disk import DiskReader
from mabel import Reader
try:
    from rich import traceback
    traceback.install()
except ImportError:   # pragma: no cover
    pass

from mabel.logging import get_logger
get_logger().setLevel(5)


def test_block_parser():
    r = list(Reader(
            inner_reader=DiskReader,
            dataset='tests/data/tweets',
            row_format="block",
            raw_path=True))

    block = '\n'.join(r)
    assert len(r) == 2
    assert len(block.splitlines()) == 50

def test_text_parser():
    r = list(Reader(
            inner_reader=DiskReader,
            dataset='tests/data/tweets',
            row_format="text",
            raw_path=True))

    assert len(r) == 50
    assert isinstance(r[0], str)

def test_json_parser():
    r = list(Reader(
            inner_reader=DiskReader,
            dataset='tests/data/tweets',
            row_format="json",
            raw_path=True))

    assert len(r) == 50
    assert isinstance(r[0], dict)

def test_default_parser():
    r = list(Reader(
            inner_reader=DiskReader,
            dataset='tests/data/tweets',
            raw_path=True))

    assert len(r) == 50
    assert isinstance(r[0], dict)


if __name__ == "__main__":
    test_block_parser()
    test_text_parser()
    test_json_parser()
    test_default_parser()

    print('okay')
    