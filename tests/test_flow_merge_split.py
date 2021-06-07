"""
Tests for the execution of flows. Create a basic flow
and push a payload through it.
"""
import os
import sys
import pytest

sys.path.insert(1, os.path.join(sys.path[0], ".."))

from mabel import operator
from mabel.flows import Flow
from mabel.errors import FlowError
from mabel.operators import EndOperator, NoOpOperator
from rich import traceback

traceback.install()


@operator
def p(data):
    print(data)
    return data


class ReplayOperator(NoOpOperator):
    def __init__(self, *args):
        super().__init__()
        self.values = list(*args)

    def execute(self, data={}, context={}):
        for value in self.values:
            yield value, context


class AdderOperator(NoOpOperator):
    def __init__(self, add):
        super().__init__()
        self.add = add

    def execute(self, data={}, context={}):
        return data + self.add, context


def test_merge_flows():

    player_one = ReplayOperator(range(10))
    player_two = ReplayOperator(range(5))

    flow = [player_one, player_two] >> p >> EndOperator()

    with flow as runner:
        runner()

    assert runner.cycles == 32, runner.cycles


def test_split_merge_flows():

    feeder = ReplayOperator(range(10))
    add_one = AdderOperator(1)
    add_ten = AdderOperator(10)

    flow = feeder >> [add_one, add_ten] >> p >> EndOperator()

    with flow as runner:
        runner()

    assert runner.cycles == 61, runner.cycles


if __name__ == "__main__":
    test_merge_flows()
    test_split_merge_flows()
