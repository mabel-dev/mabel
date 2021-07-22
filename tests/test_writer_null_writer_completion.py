import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.adapters.null import NullWriter
from mabel.data import BatchWriter
from rich import traceback
from mabel import BaseOperator
from mabel.operators import EndOperator
from mabel.operators.null import NullBatchWriterOperator

class FailOnT800Operator(BaseOperator):
    def execute(self, data: dict, context: dict):
        if data.get('name') == "T-800":
            raise Exception("Terminator")
        return data, context

class FeedMeRobotsOperator(BaseOperator):
    def execute(self, data: dict, context: dict):
        for robot in ROBOTS:
            yield robot, context


traceback.install()

ROBOTS = [
    {"name": "K-2SO"},
    {"name": "Jonny-5"},
    {"name": "Astro Boy"},
    {"name": "Vision"},
    {"name": "Ava"},
    {"name": "Baymax"},
    {"name": "WALL-E"},
    {"name": "T-800"},
]


def test_null_writer():
    w = BatchWriter(inner_writer=NullWriter, dataset="nowhere")
    for bot in ROBOTS:
        w.append(bot)
    assert w.finalize(has_failure=True) == -1


def test_terminating_flow():
    flow = FeedMeRobotsOperator() >> FailOnT800Operator() >> NullBatchWriterOperator(dataset="NOWHERE") >> EndOperator()
    with flow as runner:
        runner(None, None, 0)

if __name__ == "__main__":  # pragma: no cover
    test_null_writer()
    test_terminating_flow()

    print("okay")
