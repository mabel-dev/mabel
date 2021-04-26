"""
Test Filter Operator
"""
import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from mabel.operators import FilterOperator
from rich import traceback

traceback.install()

def test_filter_operator_default():

    in_d = {'a':1}
    n = FilterOperator()
    d, c = n.execute(in_d)

    assert d == in_d

def test_filter_operator():

    ds = [
        {"value":1},
        {"value":2},
        {"value":3},
        {"value":4}
    ]
    def is_even(val):
        return val.get('value') % 2 == 0
    op = FilterOperator(condition=is_even)
    res = [op(row) for row in ds]

    assert res[0] is None
    assert res[1] == ({'value': 2}, {})
    assert res[2] is None
    assert res[3] == ({'value': 4}, {})


if __name__ == "__main__":  # pragma: no cover
    test_filter_operator_default()
    test_filter_operator()

    print('okay')