import sys
import os

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.data.formats import dictset
from rich import traceback

traceback.install()


def test_select_record_fields():
    record = {"a": 1, "b": 2, "c": 3}
    result = dictset.select_record_fields(record, ["b", "e"])

    assert sorted(list(result.keys())) == sorted(["b", "e"])
    assert result["b"] == record["b"]
    assert result["e"] is None


def test_order():
    dict_record = {"c": 3, "a": 1, "b": 2}
    ordered_dict = dictset.order(dict_record)

    assert ordered_dict == {"a": 1, "b": 2, "c": 3}


def test_join_inner():
    set_1 = [{"key": 1, "value": "one"}, {"key": 2, "value": "two"}]
    set_2 = [{"key": 1, "plus_one": "two"}, {"key": 3, "plus_one": "four"}]
    inner = list(dictset.join(set_1, set_2, column="key", join_type="INNER"))

    assert len(inner) == 1
    assert inner[0].get("key") == 1
    assert inner[0].get("value") == "one"
    assert inner[0].get("plus_one") == "two"


def test_join_left():
    set_1 = [{"key": 1, "value": "one"}, {"key": 2, "value": "two"}]
    set_2 = [{"key": 1, "plus_one": "two"}, {"key": 3, "plus_one": "four"}]
    left = list(dictset.join(set_1, set_2, column="key", join_type="LEFT"))

    assert len(left) == 2
    assert left[0].get("key") == 1
    assert left[0].get("value") == "one"
    assert left[0].get("plus_one") == "two"
    assert left[1].get("key") == 2
    assert left[1].get("value") == "two"
    assert left[1].get("plus_one") is None


def test_union():
    set_1 = [{"key": 1, "value": "one"}, {"key": 2, "value": "two"}]
    set_2 = [{"key": 3, "value": "three"}, {"key": 4, "value": "four"}]
    union = list(dictset.union(set_1, set_2))

    assert len(union) == 4


def test_create_index():
    ds = [
        {"key": 1, "value": "one", "plus1": 2},
        {"key": 2, "value": "two", "plus1": 3},
        {"key": 3, "value": "three", "plus1": 4},
        {"key": 4, "value": "four", "plus1": 5},
    ]
    idx = dictset.create_index(ds, "key")

    assert len(idx) == len(ds)
    for key in idx:
        assert isinstance(idx[key], dict)
        assert idx[key].get("key") == key


def test_select_from():
    ds = [
        {"key": 1, "value": "one", "plus1": 2},
        {"key": 2, "value": "two", "plus1": 3},
        {"key": 3, "value": "three", "plus1": 4},
        {"key": 4, "value": "four", "plus1": 5},
    ]

    selected = list(dictset.select_from(ds, ["key", "value"], lambda r: r["key"] < 3))

    assert len(selected) == 2
    assert selected[0].get("plus1") is None
    assert selected[0].get("key") is not None
    assert selected[0].get("value") is not None

    selected = list(dictset.select_from(ds))

    assert len(selected) == 4


def test_set_column_func():
    ds = [
        {"key": 1, "value": "one", "plus1": 2},
        {"key": 2, "value": "two", "plus1": 3},
        {"key": 3, "value": "three", "plus1": 4},
        {"key": 4, "value": "four", "plus1": 5},
    ]
    updated = list(dictset.set_column(ds, "plus2", lambda r: r["key"] + 2))

    assert len(updated) == len(ds)
    for row in updated:
        assert row.get("key") is not None
        assert row.get("plus2") == row.get("key") + 2


def test_set_column_const():
    ds = [
        {"key": 1, "value": "one", "plus1": 2},
        {"key": 2, "value": "two", "plus1": 3},
        {"key": 3, "value": "three", "plus1": 4},
        {"key": 4, "value": "four", "plus1": 5},
    ]
    updated = list(dictset.set_column(ds, "plus2", 2))

    assert len(updated) == len(ds)
    for row in updated:
        assert row.get("key") is not None
        assert row.get("plus2") == 2


def test_distinct():
    ds = [
        {"key": 1, "value": "one", "plus1": 2},
        {"key": 2, "value": "two", "plus1": 3},
        {"key": 1, "value": "one", "plus1": 2},
        {"key": 3, "value": "three", "plus1": 4},
        {"key": 1, "value": "one", "plus1": 2},
        {"key": 4, "value": "four", "plus1": 5},
        {"key": 1, "value": "one", "plus1": 2},
    ]

    distinct = list(dictset.drop_duplicates(ds))

    assert len(distinct) == 4


def test_limit():
    ds = [
        {"key": 1, "value": "one", "plus1": 2},
        {"key": 2, "value": "two", "plus1": 3},
        {"key": 3, "value": "three", "plus1": 4},
        {"key": 4, "value": "four", "plus1": 5},
    ]
    limit = list(dictset.limit(ds, 2))

    assert len(limit) == 2


def test_match():
    set_1 = [
        {"key": 1, "value": "one", "plus1": 2},
        {"key": 2, "value": "two", "plus1": 3},
        {"key": 3, "value": "three", "plus1": 4},
        {"key": 4, "value": "four", "plus1": 5},
    ]
    set_2 = [
        {"key": 1, "value": "one", "plus1": 2},
        {"key": 4, "value": "four", "plus1": 5},
        {"key": 2, "value": "two", "plus1": 3},
        {"key": 3, "value": "three", "plus1": 4},
    ]
    set_3 = [
        {"key": 1, "value": "one", "plus1": 2},
        {"key": 2, "value": "four", "plus1": 5},
        {"key": 3, "value": "two", "plus1": 3},
        {"key": 4, "value": "three", "plus1": 4},
    ]

    assert dictset.dictsets_match(set_1, set_2)
    assert not dictset.dictsets_match(set_1, set_3)


def test_paging():
    ds = []
    for i in range(100):
        row = {"key": i}
        ds.append(row)

    page_size = 10
    for index, page in enumerate(dictset.page_dictset(ds, page_size)):
        assert len(page) <= page_size
        assert page == ds[index * page_size : (index + 1) * page_size]


def test_sort():
    ds = [
        {"key": 6},
        {"key": 10},
        {"key": 3},
        {"key": 9},
        {"key": 8},
        {"key": 4},
        {"key": 7},
        {"key": 5},
        {"key": 2},
        {"key": 1},
    ]

    s = list(dictset.sort(ds, "key", 4))
    # are the items ordered correctly
    for i, r in enumerate(s):
        assert 10 - i == r.get("key")

    rev = list(dictset.sort(ds, "key", 40, False))
    # the the items ordered correctly
    for i, r in enumerate(rev):
        assert 1 + i == r.get("key"), f"{i}  {r.get('key')}"


def test_to_pandas():
    ds = [
        {"key": 1, "value": "one", "plus1": 2},
        {"key": 2, "value": "two", "plus1": 3},
        {"key": 3, "value": "three", "plus1": 4},
        {"key": 4, "value": "four", "plus1": 5},
    ]
    df = dictset.to_pandas(ds)

    assert len(df) == 4
    # if loaded correctly we should be able to operate on the values
    assert df["plus1"].sum() == 14


def test_extract_column():
    ds = [
        {"key": 1, "value": "one"},
        {"key": 2, "value": "two"},
        {"key": 3, "value": "three"},
        {"key": 4, "value": "four"},
        {"key": 5, "value": "five"},
    ]

    assert dictset.extract_column(ds, "value") == [
        "one",
        "two",
        "three",
        "four",
        "five",
    ]


def test_counter():
    ds = [
        {"key": 1, "value": "one"},
        {"key": 2, "value": "two"},
        {"key": 3, "value": "three"},
        {"key": 4, "value": "four"},
        {"key": 5, "value": "five"},
    ]

    l = []
    try:
        for i in dictset.pass_thru_counter(ds):
            l.append(i)
    except Exception as r:
        count = r.args[0]

    assert l == ds
    assert count == 5


def test_jsonify():
    jsonl = [
        '{"key": 0}',
        '{"key": 1}',
        '{"key": 2}',
        '{"key": 3}',
        '{"key": 4}',
        '{"key": 5}',
    ]

    ds = dictset.jsonify(jsonl)
    for i, r in enumerate(ds):
        assert isinstance(r, dict)
        assert r["key"] == i


if __name__ == "__main__":  # pragma: no cover
    test_select_record_fields()
    test_order()
    test_join_inner()
    test_join_left()
    test_union()
    test_create_index()
    test_select_from()
    test_set_column_func()
    test_set_column_const()
    test_distinct()
    test_limit()
    test_match()
    test_paging()
    test_sort()
    test_to_pandas()
    test_extract_column()
    test_counter()
    test_jsonify()

    print("okay")
