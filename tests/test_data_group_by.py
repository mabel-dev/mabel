import sys
import os

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.data.internals.group_by import GroupBy
from mabel.data.internals.dictset import STORAGE_CLASS, DictSet
from rich import traceback

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
    assert list(GroupBy(ds, "user").count()) == [{'COUNT(*)': 6, 'user': 'alice'}, {'COUNT(*)': 5, 'user': 'bob'}, {'COUNT(*)': 2, 'user': 'eve'}]

    ds = DictSet(data, storage_class=STORAGE_CLASS.MEMORY)
    assert list(GroupBy(ds, "user").average("value")) == [{'AVG(value)': 4.0, 'user': 'alice'}, {'AVG(value)': 1.4, 'user': 'bob'}, {'AVG(value)': 6.5, 'user': 'eve'}]

    ds = DictSet(data, storage_class=STORAGE_CLASS.MEMORY)
    assert list(GroupBy(ds, "user").max("value")) == [{'MAX(value)': 5, 'user': 'alice'}, {'MAX(value)': 2, 'user': 'bob'}, {'MAX(value)': 7, 'user': 'eve'}]

    ds = DictSet(data, storage_class=STORAGE_CLASS.MEMORY)
    assert list(GroupBy(ds, "user").min("value")) == [{'MIN(value)': 3, 'user': 'alice'}, {'MIN(value)': 1, 'user': 'bob'}, {'MIN(value)': 6, 'user': 'eve'}]
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
    assert gs == [{'fname': 'bob', 'sname': 'smith'}, {'fname': 'bob', 'sname': 'jones'}, {'fname': 'alice', 'sname': 'jones'}, {'fname': 'alice', 'sname': 'smith'}, {'fname': 'eve', 'sname': 'jones'}, {'fname': 'eve', 'sname': 'smith'}], gs

    ds = DictSet(data, storage_class=STORAGE_CLASS.MEMORY)
    gs = list(GroupBy(ds, ("fname", "sname")).count())
    assert gs ==  [{'COUNT(*)': 2, 'fname': 'bob', 'sname': 'jones'}, {'COUNT(*)': 3, 'fname': 'bob', 'sname': 'smith'}, {'COUNT(*)': 3, 'fname': 'alice', 'sname': 'smith'}, {'COUNT(*)': 1, 'fname': 'eve', 'sname': 'smith'}, {'COUNT(*)': 1, 'fname': 'eve', 'sname': 'jones'}, {'COUNT(*)': 3, 'fname': 'alice', 'sname': 'jones'}], gs

    ds = DictSet(data, storage_class=STORAGE_CLASS.MEMORY)
    gs = list(GroupBy(ds, ("fname", "sname")).average('value'))
    assert gs == [{'AVG(value)': 2.0, 'fname': 'bob', 'sname': 'jones'}, {'AVG(value)': 1.0, 'fname': 'bob', 'sname': 'smith'}, {'AVG(value)': 4.333333333333333, 'fname': 'alice', 'sname': 'smith'}, {'AVG(value)': 7.0, 'fname': 'eve', 'sname': 'smith'}, {'AVG(value)': 6.0, 'fname': 'eve', 'sname': 'jones'}, {'AVG(value)': 3.6666666666666665, 'fname': 'alice', 'sname': 'jones'}], gs

    ds = DictSet(data, storage_class=STORAGE_CLASS.MEMORY)
    gs = list(GroupBy(ds, ("fname", "sname")).average('cost'))
    assert gs == [{'AVG(cost)': 3.0, 'fname': 'bob', 'sname': 'jones'}, {'AVG(cost)': 1.6666666666666667, 'fname': 'bob', 'sname': 'smith'}, {'AVG(cost)': 2.3333333333333335, 'fname': 'alice', 'sname': 'smith'}, {'AVG(cost)': 1.0, 'fname': 'eve', 'sname': 'smith'}, {'AVG(cost)': 2.0, 'fname': 'eve', 'sname': 'jones'}, {'AVG(cost)': 3.3333333333333335, 'fname': 'alice', 'sname': 'jones'}], gs

    ds = DictSet(data, storage_class=STORAGE_CLASS.MEMORY)
    gs = list(GroupBy(ds, ("fname", "sname")).average(('cost', 'value',)))
    assert gs == [{'AVG(cost)': 3.0, 'AVG(value)': 2.0, 'fname': 'bob', 'sname': 'jones'}, {'AVG(cost)': 1.6666666666666667, 'AVG(value)': 1.0, 'fname': 'bob', 'sname': 'smith'}, {'AVG(cost)': 2.3333333333333335, 'AVG(value)': 4.333333333333333, 'fname': 'alice', 'sname': 'smith'}, {'AVG(cost)': 1.0, 'AVG(value)': 7.0, 'fname': 'eve', 'sname': 'smith'}, {'AVG(cost)': 2.0, 'AVG(value)': 6.0, 'fname': 'eve', 'sname': 'jones'}, {'AVG(cost)': 3.3333333333333335, 'AVG(value)': 3.6666666666666665, 'fname': 'alice', 'sname': 'jones'}], gs

    ds = DictSet(data, storage_class=STORAGE_CLASS.MEMORY)
    gs = list(GroupBy(ds, ("fname", "sname")).aggregate([('MAX', 'value'),('MIN', 'cost')]))
    assert gs == [{'MAX(value)': 2, 'MIN(cost)': 2, 'fname': 'bob', 'sname': 'jones'}, {'MAX(value)': 1, 'MIN(cost)': 1, 'fname': 'bob', 'sname': 'smith'}, {'MAX(value)': 5, 'MIN(cost)': 1, 'fname': 'alice', 'sname': 'smith'}, {'MAX(value)': 7, 'MIN(cost)': 1, 'fname': 'eve', 'sname': 'smith'}, {'MAX(value)': 6, 'MIN(cost)': 2, 'fname': 'eve', 'sname': 'jones'}, {'MAX(value)': 5, 'MIN(cost)': 2, 'fname': 'alice', 'sname': 'jones'}], gs

    # fmt:on


if __name__ == "__main__":  # pragma: no cover
    test_group_by()
    test_combined_group_by()

    print("okay")
