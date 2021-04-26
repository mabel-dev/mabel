import datetime
import sys
import os
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from mabel.data.formats import dictset
from rich import traceback

traceback.install()

def summarize(ds):
    res = []
    for row in ds:
        res.append(row['value'])
    return {
        'count': len(res),
        'min': min(res),
        'max': max(res)
    }

def test_group_by_advanced():
    ds = [
        {'user': 'bob',   'value': 1},
        {'user': 'bob',   'value': 2},
        {'user': 'bob',   'value': 1},
        {'user': 'bob',   'value': 2},
        {'user': 'bob',   'value': 1},
        {'user': 'alice', 'value': 3},
        {'user': 'alice', 'value': 4},
        {'user': 'alice', 'value': 3},
        {'user': 'alice', 'value': 4},
        {'user': 'alice', 'value': 5},
        {'user': 'alice', 'value': 5},
        {'user': 'eve',   'value': 6},
        {'user': 'eve',   'value': 7}
    ]

    groups = dictset.group_by(ds, 'user').apply(summarize)
    assert groups == {'bob': {'count': 5, 'min': 1, 'max': 2}, 'alice': {'count': 6, 'min': 3, 'max': 5}, 'eve': {'count': 2, 'min': 6, 'max': 7}}


def test_group_by():
    ds = [
        {'user': 'bob',   'value': 1},
        {'user': 'bob',   'value': 2},
        {'user': 'alice', 'value': 3},
        {'user': 'alice', 'value': 4},
        {'user': 'alice', 'value': 5},
        {'user': 'eve',   'value': 6},
        {'user': 'eve',   'value': 7}
    ]

    groups = dictset.group_by(ds, 'user')

    # the right number of groups
    assert len(groups) == 3

    # the groups have the right number of records
    assert groups.count('bob') == 2
    assert groups.count('alice') == 3
    assert groups.count('eve') == 2

    # the aggregations work
    assert groups.aggregate('value', max).get('bob') == 2
    assert groups.aggregate('value', min).get('alice') == 3
    assert groups.aggregate('value', sum).get('eve') == 13


if __name__ == "__main__":  # pragma: no cover
    test_group_by()
    test_group_by_advanced()

    
    print('okay')
