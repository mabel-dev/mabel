import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel import Reader, DictSet
from mabel.data import STORAGE_CLASS
from mabel.adapters.disk import DiskReader
from mabel.logging import get_logger

get_logger().setLevel(5)


STORAGE_CLASSES = [
    STORAGE_CLASS.NO_PERSISTANCE,
    STORAGE_CLASS.COMPRESSED_MEMORY,
    STORAGE_CLASS.MEMORY,
    STORAGE_CLASS.DISK,
]


def get_ds(**kwargs):
    ds = Reader(
        inner_reader=DiskReader, dataset="tests/data/tweets", raw_path=True, **kwargs
    )
    return ds


def test_count():
    for storage_class in STORAGE_CLASSES:
        ds = get_ds(persistence=storage_class)
        if storage_class == STORAGE_CLASS.NO_PERSISTANCE:
            assert ds.count() == -1, f"{storage_class} {ds.count()}"
        else:
            assert ds.count() == 50, f"{storage_class} {ds.count()}"


def test_enumeration():
    for storage_class in STORAGE_CLASSES:
        ds = get_ds(persistence=storage_class)
        i = -1
        for i, r in enumerate(ds):
            pass
        assert i + 1 == 50, f"{storage_class} {i+1}"


def test_sample():
    for storage_class in STORAGE_CLASSES:
        ds = get_ds(persistence=storage_class)
        sample = ds.sample(0.02)
        assert isinstance(sample, DictSet)
        assert sample.count() < 5, sample.count()
        assert sample.storage_class == storage_class


def test_repr():
    for storage_class in STORAGE_CLASSES:
        ds = get_ds(persistence=storage_class)
        rep = repr(ds)
        assert "├" in rep, storage_class


def test_collect():
    for storage_class in STORAGE_CLASSES:
        ds = get_ds(persistence=storage_class)
        collection = ds.collect_list("username")
        assert collection.count("NBCNews") == 44, storage_class


def test_keys():
    for storage_class in STORAGE_CLASSES:
        ds = get_ds(persistence=storage_class)
        keys = ds.keys()
        assert keys == [
            "userid",
            "username",
            "user_verified",
            "followers",
            "tweet",
            "location",
            "sentiment",
            "timestamp",
        ], storage_class


def test_types():
    for storage_class in STORAGE_CLASSES:
        if storage_class != STORAGE_CLASS.NO_PERSISTANCE:
            ds = get_ds(persistence=storage_class)
            types = ds.types()
            assert types["userid"] == "int", types
            assert types["username"] == "str"
            assert types["user_verified"] == "bool"
            assert types["followers"] == "int"
            assert types["tweet"] == "str"
            assert types["location"] == "str"
            assert types["sentiment"] == "numeric"
            assert types["timestamp"] == "str"


def test_distinct():
    for storage_class in STORAGE_CLASSES:
        if storage_class != STORAGE_CLASS.NO_PERSISTANCE:
            ds = get_ds(persistence=storage_class)
            assert ds.distinct().count() == 50, storage_class
            assert ds.distinct("username").count() == 2, storage_class
            assert ds.distinct("username", "location").count() == 2, storage_class
            assert ds.distinct("sentiment").count() == 36, storage_class


def test_summary():
    data = [
        {"key": 1, "value": "one", "plus1": 2},
        {"key": 2, "value": "two", "plus1": 3},
        {"key": 3, "value": "three", "plus1": 4},
        {"key": 4, "value": "four", "plus1": 5},
    ]
    ds = DictSet(data, storage_class=STORAGE_CLASS.MEMORY)

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
    ds = DictSet(data, storage_class=STORAGE_CLASS.MEMORY)
    assert ds.take(1).collect_list().pop() == {"key": 1, "value": "one", "plus1": 2}
    assert ds.take(2).count() == 2, ds.take(2).count()


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

    print("OKAY")
