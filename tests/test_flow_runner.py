"""
Tests for the execution of flows. Create a basic flow
and push a payload through it.
"""
import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from mabel.logging import get_logger
from mabel.operators import EndOperator, NoOpOperator
try:
    from rich import traceback
    traceback.install()
except ImportError:
    pass


def test_flow_runner():
    """
    Run a basic flow
    """
    e = EndOperator()
    n = NoOpOperator()
    o = NoOpOperator()
    flow = n > o > e

    errored = False
    try:
        with flow as runner:
            runner.run(data="What is my purpose?")
            runner.run(data="You pass butter.")
            runner.run(data="Oh my God.")
            runner.run(data="Yeah, welcome to the club, pal.")

    except Exception:
        errored = True

    assert not errored


if __name__ == "__main__":

    test_flow_runner()

    print('okay')
