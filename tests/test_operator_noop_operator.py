"""
Test No Operation Operator
"""
import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from mabel.operators import NoOpOperator
try:
    from rich import traceback
    traceback.install()
except ImportError:
    pass


def test_noop_operator():

    in_d = {'a':1}
    in_c = {'b':2}

    n = NoOpOperator(print_message=True)
    d, c = n.execute(in_d,in_c)

    assert d == in_d
    assert c == in_c


if __name__ == "__main__":
    test_noop_operator()

    print('okay')