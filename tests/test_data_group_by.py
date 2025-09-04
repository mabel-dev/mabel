import sys
import os

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.data.internals.group_by import GroupBy
from mabel.data.internals.dictset import STORAGE_CLASS, DictSet
from rich import traceback
from decimal import Decimal

traceback.install()


def test_group_by():
    """
    Test simple grouping and aggregation
    """
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

    # fmt:off
    ds = DictSet(data, storage_class=STORAGE_CLASS.MEMORY)
    assert list(GroupBy(ds, "user").groups()) == [{'user': 'bob'}, {'user': 'alice'}, {'user': 'eve'}]

    ds = DictSet(data, storage_class=STORAGE_CLASS.MEMORY)
    gb = GroupBy(ds, "user")
    cn = gb.count()
    ls = list(cn)
    expected = [{'COUNT(*)': 6, 'user': 'alice'}, {'COUNT(*)': 5, 'user': 'bob'}, {'COUNT(*)': 2, 'user': 'eve'}]
    assert set(tuple(sorted(d.items())) for d in ls) == set(tuple(sorted(d.items())) for d in expected)

    ds = DictSet(data, storage_class=STORAGE_CLASS.MEMORY)
    gb = list(GroupBy(ds, "user").average("value"))
    expected = [{'AVG(value)': Decimal("4.0"), 'user': 'alice'}, {'AVG(value)': Decimal("1.4"), 'user': 'bob'}, {'AVG(value)': Decimal("6.5"), 'user': 'eve'}]
    assert set(tuple(sorted(d.items())) for d in gb) == set(tuple(sorted(d.items())) for d in expected)

    ds = DictSet(data, storage_class=STORAGE_CLASS.MEMORY)
    gs = list(GroupBy(ds, "user").max("value"))
    expected = [{'MAX(value)': 5, 'user': 'alice'}, {'MAX(value)': 2, 'user': 'bob'}, {'MAX(value)': 7, 'user': 'eve'}]
    assert set(tuple(sorted(d.items())) for d in gs) == set(tuple(sorted(d.items())) for d in expected)

    ds = DictSet(data, storage_class=STORAGE_CLASS.MEMORY)
    gs = list(GroupBy(ds, "user").min("value"))
    expected = [{'MIN(value)': 3, 'user': 'alice'}, {'MIN(value)': 1, 'user': 'bob'}, {'MIN(value)': 6, 'user': 'eve'}]
    assert set(tuple(sorted(d.items())) for d in gs) == set(tuple(sorted(d.items())) for d in expected)

    # fmt:on


def test_combined_group_by():
    """
    Test combined grouping and aggregation
    """
    data = [
        {"fname": "bob", "sname": "smith", "value": 1, "cost": 1},
        {"fname": "bob", "sname": "jones", "value": 2, "cost": 4},
        {"fname": "bob", "sname": "smith", "value": 1, "cost": 3},
        {"fname": "bob", "sname": "jones", "value": 2, "cost": 2},
        {"fname": "bob", "sname": "smith", "value": 1, "cost": 1},
        {"fname": "alice", "sname": "jones", "value": 3, "cost": 4},
        {"fname": "alice", "sname": "smith", "value": 4, "cost": 3},
        {"fname": "alice", "sname": "jones", "value": 3, "cost": 2},
        {"fname": "alice", "sname": "smith", "value": 4, "cost": 1},
        {"fname": "alice", "sname": "jones", "value": 5, "cost": 4},
        {"fname": "alice", "sname": "smith", "value": 5, "cost": 3},
        {"fname": "eve", "sname": "jones", "value": 6, "cost": 2},
        {"fname": "eve", "sname": "smith", "value": 7, "cost": 1},
    ]

    # fmt:off
    ds = DictSet(data, storage_class=STORAGE_CLASS.MEMORY)
    gs = list(GroupBy(ds, ("fname", "sname")).groups())
    expected = [{'fname': 'bob', 'sname': 'smith'}, {'fname': 'bob', 'sname': 'jones'}, {'fname': 'alice', 'sname': 'jones'}, {'fname': 'alice', 'sname': 'smith'}, {'fname': 'eve', 'sname': 'jones'}, {'fname': 'eve', 'sname': 'smith'}]
    assert set(tuple(sorted(d.items())) for d in gs) == set(tuple(sorted(d.items())) for d in expected)

    ds = DictSet(data, storage_class=STORAGE_CLASS.MEMORY)
    gs = list(GroupBy(ds, ("fname", "sname")).count())
    expected = [{'COUNT(*)': 2, 'fname': 'bob', 'sname': 'jones'}, {'COUNT(*)': 3, 'fname': 'bob', 'sname': 'smith'}, {'COUNT(*)': 3, 'fname': 'alice', 'sname': 'smith'}, {'COUNT(*)': 1, 'fname': 'eve', 'sname': 'smith'}, {'COUNT(*)': 1, 'fname': 'eve', 'sname': 'jones'}, {'COUNT(*)': 3, 'fname': 'alice', 'sname': 'jones'}]
    assert set(tuple(sorted(d.items())) for d in gs) == set(tuple(sorted(d.items())) for d in expected)

    ds = DictSet(data, storage_class=STORAGE_CLASS.MEMORY)
    gs = list(GroupBy(ds, ("fname", "sname")).average('value'))
    expected = [{'AVG(value)': Decimal('2.0'), 'fname': 'bob', 'sname': 'jones'}, {'AVG(value)': Decimal('1.0'), 'fname': 'bob', 'sname': 'smith'}, {'AVG(value)': Decimal('4.333333333333333333333333333'), 'fname': 'alice', 'sname': 'smith'}, {'AVG(value)': Decimal('7.0'), 'fname': 'eve', 'sname': 'smith'}, {'AVG(value)': Decimal('6.0'), 'fname': 'eve', 'sname': 'jones'}, {'AVG(value)': Decimal('3.666666666666666666666666667'), 'fname': 'alice', 'sname': 'jones'}]
    assert set(tuple(sorted(d.items())) for d in gs) == set(tuple(sorted(d.items())) for d in expected)


    ds = DictSet(data, storage_class=STORAGE_CLASS.MEMORY)
    gs = list(GroupBy(ds, ("fname", "sname")).average('cost'))
    expected = [{'AVG(cost)': Decimal('3.0'), 'fname': 'bob', 'sname': 'jones'}, {'AVG(cost)': Decimal('1.666666666666666666666666667'), 'fname': 'bob', 'sname': 'smith'}, {'AVG(cost)': Decimal('2.333333333333333333333333333'), 'fname': 'alice', 'sname': 'smith'}, {'AVG(cost)': Decimal('1.0'), 'fname': 'eve', 'sname': 'smith'}, {'AVG(cost)': Decimal('2.0'), 'fname': 'eve', 'sname': 'jones'}, {'AVG(cost)': Decimal('3.333333333333333333333333333'), 'fname': 'alice', 'sname': 'jones'}]
    assert set(tuple(sorted(d.items())) for d in gs) == set(tuple(sorted(d.items())) for d in expected)

    ds = DictSet(data, storage_class=STORAGE_CLASS.MEMORY)
    gs = list(GroupBy(ds, ("fname", "sname")).average(('cost', 'value',)))
    expected = [{'AVG(cost)': Decimal('3'), 'AVG(value)': Decimal('2'), 'fname': 'bob', 'sname': 'jones'}, {'AVG(cost)': Decimal('1.666666666666666666666666667'), 'AVG(value)': Decimal('1'), 'fname': 'bob', 'sname': 'smith'}, {'AVG(cost)': Decimal('2.333333333333333333333333333'), 'AVG(value)': Decimal('4.333333333333333333333333333'), 'fname': 'alice', 'sname': 'smith'}, {'AVG(cost)': 1.0, 'AVG(value)': 7.0, 'fname': 'eve', 'sname': 'smith'}, {'AVG(cost)': 2.0, 'AVG(value)': 6.0, 'fname': 'eve', 'sname': 'jones'}, {'AVG(cost)': Decimal('3.333333333333333333333333333'), 'AVG(value)': Decimal('3.666666666666666666666666667'), 'fname': 'alice', 'sname': 'jones'}]
    assert set(tuple(sorted(d.items())) for d in gs) == set(tuple(sorted(d.items())) for d in expected)

    ds = DictSet(data, storage_class=STORAGE_CLASS.MEMORY)
    gs = list(GroupBy(ds, ("fname", "sname")).aggregate([('MAX', 'value'),('MIN', 'cost')]))
    expected = [{'MAX(value)': 2, 'MIN(cost)': 2, 'fname': 'bob', 'sname': 'jones'}, {'MAX(value)': 1, 'MIN(cost)': 1, 'fname': 'bob', 'sname': 'smith'}, {'MAX(value)': 5, 'MIN(cost)': 1, 'fname': 'alice', 'sname': 'smith'}, {'MAX(value)': 7, 'MIN(cost)': 1, 'fname': 'eve', 'sname': 'smith'}, {'MAX(value)': 6, 'MIN(cost)': 2, 'fname': 'eve', 'sname': 'jones'}, {'MAX(value)': 5, 'MIN(cost)': 2, 'fname': 'alice', 'sname': 'jones'}]
    assert set(tuple(sorted(d.items())) for d in gs) == set(tuple(sorted(d.items())) for d in expected)

    # fmt:on


def test_gappy_set():
    data = [
        {"key": 1, "value": "one", "plus1": 2},
        {"key": 2, "value": "two", "plus1": 3},
        {"key": None, "value": "one", "plus1": 4},
        {"key": 4, "value": "two", "plus1": 5},
        {"key": 4, "value": None, "plus1": 5},
    ]
    ds = DictSet(data, storage_class=STORAGE_CLASS.MEMORY)
    gs = list(ds.group_by("value").average("key"))
    expected = [
        {"AVG(key)": 4.0, "value": None},
        {"AVG(key)": 3.0, "value": "two"},
        {"AVG(key)": 1.0, "value": "one"},
    ]

    assert set(tuple(sorted(d.items())) for d in gs) == set(tuple(sorted(d.items())) for d in expected)


if __name__ == "__main__":  # pragma: no cover
    from helpers.runner import run_tests

    run_tests()
