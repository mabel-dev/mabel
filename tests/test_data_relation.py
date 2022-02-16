import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel import Reader, Relation
from mabel.data.types import MABEL_TYPES
from mabel.adapters.disk import DiskReader
from mabel.logging import get_logger

get_logger().setLevel(5)


def get_ds(**kwargs):
    ds = Reader(
        inner_reader=DiskReader, dataset="tests/data/tweets", partitioning=[], **kwargs
    )
    return ds


def test_count():
    ds = get_ds()
    assert ds.count() == 50, f"{ds.count()}"


def test_enumeration():
    ds = get_ds()
    i = -1
    for i, r in enumerate(ds):
        pass
    assert i + 1 == 50, f"{i+1}"


def test_sample():
    ds = get_ds()
    sample = ds.sample(0.50)
    assert isinstance(sample, Relation)
    assert 20 < sample.count() < 30, sample.count()


def test_repr():
    ds = get_ds()
    rep = repr(ds)
    assert "├" in rep, rep


def test_collect():
    ds = get_ds()
    collection = ds.collect_list("username")
    assert collection.count("NBCNews") == 44, collection


def test_keys():
    ds = get_ds()
    keys = ds.columns
    assert keys == [
        "userid",
        "username",
        "user_verified",
        "followers",
        "tweet",
        "location",
        "sentiment",
        "timestamp",
    ]


def test_types():
    ds = get_ds()
    types = ds.schema
    assert types["userid"]["type"] == MABEL_TYPES.NUMERIC, types["userid"]
    assert types["username"]["type"] == MABEL_TYPES.VARCHAR
    assert types["user_verified"]["type"] == MABEL_TYPES.BOOLEAN
    assert types["followers"]["type"] == MABEL_TYPES.NUMERIC
    assert types["tweet"]["type"] == MABEL_TYPES.VARCHAR
    assert types["location"]["type"] == MABEL_TYPES.VARCHAR
    assert types["sentiment"]["type"] == MABEL_TYPES.NUMERIC
    assert types["timestamp"]["type"] == MABEL_TYPES.VARCHAR


def test_distinct():
    ds = get_ds()
    assert ds.distinct().count() == 50, ds.distinct().count() 
    assert ds["username"].distinct().count() == 2, ds["username"].distinct().count()
    assert ds["username", "location"].distinct().count() == 2
    assert ds["sentiment"].distinct().count() == 36


def test_summary():
    data = [
        {"key": 1, "value": "one", "plus1": 2},
        {"key": 2, "value": "two", "plus1": 3},
        {"key": 3, "value": "three", "plus1": 4},
        {"key": 4, "value": "four", "plus1": 5},
    ]
    ds = Relation(data)

    assert ds.min("key") == 1
    assert ds.max("key") == 4
    assert ds.min_max("key") == (
        1,
        4,
    )
    assert ds.sum("key") == 10
    assert ds.mean("key") == 2.5
    assert ds.standard_deviation("key") == 1.2909944487358056
    assert ds.variance("key") == 1.6666666666666667


def test_take():
    data = [
        {"key": 1, "value": "one", "plus1": 2},
        {"key": 2, "value": "two", "plus1": 3},
        {"key": 3, "value": "three", "plus1": 4},
        {"key": 4, "value": "four", "plus1": 5},
    ]
    ds = Relation(data)
    assert Relation(ds.fetchmany(1)).collect_list().pop() == {"key": 1, "value": "one", "plus1": 2}
    assert Relation(ds.fetchmany(2)).count() == 2, ds.take(2).count()


#def test_items():
#    data = [
#        {"key": 1, "value": "one", "plus1": 2},
#        {"key": 2, "value": "two", "plus1": 3},
#        {"key": 3, "value": "three", "plus1": 4},
#        {"key": 4, "value": "four", "plus1": 5},
#    ]
#    ds = Relation(data)
#    items = list([i.as_dict() for i in ds.get_items(0, 2)])
#    assert items == [
#        {"key": 1, "value": "one", "plus1": 2},
#        {"key": 3, "value": "three", "plus1": 4},
#    ], items


def test_selectors():
    data = [
        {"key": 1, "value": "one", "plus1": 2},
        {"key": 2, "value": "two", "plus1": 3},
        {"key": 3, "value": "three", "plus1": 4},
        {"key": 4, "value": "four", "plus1": 5},
    ]
    ds = Relation(data)
    dnf = ds.apply_selection(("key", "=", 1))
    assert dnf.count() == 1, dnf.count()
    assert dnf.fetchone() == {"key": 1, "value": "one", "plus1": 2}


def test_hash():
    data = [
        {"key": 1, "value": "one", "plus1": 2},
        {"key": 2, "value": "two", "plus1": 3},
        {"key": 3, "value": "three", "plus1": 4},
        {"key": 4, "value": "four", "plus1": 5},
    ]
    ds = Relation(data)
    hashval = hash(ds)
    assert hashval == 303148629686243561, hashval


def test_projection():
    data = [
        {"key": 1, "value": "one", "plus1": 2},
        {"key": 2, "value": "two", "plus1": 3},
        {"key": 3, "value": "three", "plus1": 4},
        {"key": 4, "value": "four", "plus1": 5},
    ]
    ds = Relation(data).apply_projection("key").collect_list()
    assert ds == [{"key": 1}, {"key": 2}, {"key": 3}, {"key": 4}], ds

    ds = (
        Relation(data)
        .apply_projection(
            (
                "key",
                "value",
            )
        )
        .collect_list()
    )
    assert ds == [
        {"key": 1, "value": "one"},
        {"key": 2, "value": "two"},
        {"key": 3, "value": "three"},
        {"key": 4, "value": "four"},
    ], ds


def test_inline_projection():
    data = [
        {"key": 1, "value": "one", "plus1": 2},
        {"key": 2, "value": "two", "plus1": 3},
        {"key": 3, "value": "three", "plus1": 4},
        {"key": 4, "value": "four", "plus1": 5},
    ]
    ds = Relation(data)["key"].collect_list()
    assert ds == [{"key": 1}, {"key": 2}, {"key": 3}, {"key": 4}], ds

    ds = Relation(data)["key", "value"].collect_list()
    assert ds == [
        {"key": 1, "value": "one"},
        {"key": 2, "value": "two"},
        {"key": 3, "value": "three"},
        {"key": 4, "value": "four"},
    ], ds


def test_sort():
    data = [
        {"key": 1, "value": "one", "plus1": 2},
        {"key": None, "value": "two", "plus1": 3},
        {"key": 3, "value": "three", "plus1": 4},
        {"key": 4, "value": "four", "plus1": 5},
    ]
    ds = Relation(data)
    print("TEST TEMPORARILY REMOVED")
#    st = list(ds.sort_and_take("key", 100))
#    assert st == [
#        {"key": None, "value": "two", "plus1": 3},
#        {"key": 1, "value": "one", "plus1": 2},
#        {"key": 3, "value": "three", "plus1": 4},
#        {"key": 4, "value": "four", "plus1": 5},
#    ], st


if __name__ == "__main__":
    test_count()
    test_enumeration()
    test_sample()
    test_repr()
    test_collect()
    test_keys()
    test_distinct()
    test_types()
    test_summary()
    test_take()
#    test_items()
    test_selectors()
    test_hash()
    test_sort()
    test_projection()
    test_inline_projection()

    print("OKAY")
