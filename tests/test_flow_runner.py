"""
Tests for the execution of flows. Create a basic flow
and push a payload through it.
"""
import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from mabel.logging import get_logger
from mabel.operators import EndOperator, NoOpOperator
from rich import traceback

traceback.install()


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
            runner(data="What is my purpose?")
            runner(data="You pass butter.")
            runner(data="Oh my God.")
            runner(data="Yeah, welcome to the club, pal.")

    except Exception:
        errored = True

    assert not errored


if __name__ == "__main__":  # pragma: no cover

    test_flow_runner()

    print('okay')
