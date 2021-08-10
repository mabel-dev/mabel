import sys
import os

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel import DictSet
from mabel.data import STORAGE_CLASS
from rich import traceback

traceback.install()


def test_take():
    data = [
        {"key": 1, "value": "one", "plus1": 2},
        {"key": 2, "value": "two", "plus1": 3},
        {"key": 3, "value": "three", "plus1": 4},
        {"key": 4, "value": "four", "plus1": 5},
    ]
    ds = DictSet(data, storage_class=STORAGE_CLASS.MEMORY)
    assert ds.take(1).collect().pop() == {"key": 1, "value": "one", "plus1": 2}
    assert ds.take(2).count() == 2, ds.take(2).count()


if __name__ == "__main__":  # pragma: no cover

    test_take()

    print("okay")
