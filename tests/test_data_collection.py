import sys
import os

sys.path.insert(1, os.path.join(sys.path[0], ".."))

from mabel.data.internals.dictset import STORAGE_CLASS, DictSet
from rich import traceback

traceback.install()


def summarize(ds):
    res = []
    for row in ds:
        res.append(row["value"])
    return {"count": len(res), "min": min(res), "max": max(res)}


def test_group_by_advanced():
    data = [
        {"user": "bob", "value": 1},
        {"user": "bob", "value": 2},
        {"user": "bob", "value": 1},
        {"user": "bob", "value": 2},
        {"user": "bob", "value": 1},
        {"user": "alice", "value": 3},
        {"user": "alice", "value": 4},
        {"user": "alice", "value": 3},
        {"user": "alice", "value": 4},
        {"user": "alice", "value": 5},
        {"user": "alice", "value": 5},
        {"user": "eve", "value": 6},
        {"user": "eve", "value": 7},
    ]
    ds = DictSet(data, storage_class=STORAGE_CLASS.MEMORY)

    # don't deduplicate
    groups = ds.collect_set("user", dedupe=False).apply(summarize)
    assert groups["bob"] == {"count": 5, "min": 1, "max": 2}, groups["bob"]
    assert groups["alice"] == {"count": 6, "min": 3, "max": 5}
    assert groups["eve"] == {"count": 2, "min": 6, "max": 7}

    # deduplicate
    groups = ds.collect_set("user", dedupe=True).apply(summarize)
    assert groups["bob"] == {"count": 2, "min": 1, "max": 2}
    assert groups["alice"] == {"count": 3, "min": 3, "max": 5}
    assert groups["eve"] == {"count": 2, "min": 6, "max": 7}


def test_group_by():
    data = [
        {"user": "bob", "value": 1},
        {"user": "bob", "value": 2},
        {"user": "alice", "value": 3},
        {"user": "alice", "value": 4},
        {"user": "alice", "value": 5},
        {"user": "eve", "value": 6},
        {"user": "eve", "value": 7},
    ]

    ds = DictSet(data, storage_class=STORAGE_CLASS.MEMORY)
    groups = ds.collect_set("user")

    # the right number of groups
    assert len(groups) == 3

    # the groups have the right number of records
    assert groups.count("bob") == 2
    assert groups.count("alice") == 3
    assert groups.count("eve") == 2

    # the aggregations work
    assert groups.aggregate("value", max).get("bob") == 2
    assert groups.aggregate("value", min).get("alice") == 3
    assert groups.aggregate("value", sum).get("eve") == 13


if __name__ == "__main__":  # pragma: no cover
    from tests.helpers.runner import run_tests

    run_tests()
