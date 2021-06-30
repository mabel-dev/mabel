import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.logging import get_logger
from mabel.flows.internals.base_operator import BaseOperator
from mabel.operators import EndOperator, NoOpOperator
from rich import traceback

traceback.install()

FIGHT_CLUB_RULES = [
            "The first rule of Fight Club is: You do not talk about Fight Club.",
            "The second rule of Fight Club is: You do not talk about Fight Club.",
            "Third rule of Fight Club: Someone yells 'Stop!', goes limp, taps out, the fight is over.",
            "Fourth rule: Only two guys to a fight.",
            "Fifth rule: One fight at a time, fellas.",
            "Sixth rule: No shirts, no shoes.",
            "Seventh rule: Fights will go on as long as they have to.",
            "And the eighth and final rule: If this is your first night at Fight Club, you have to fight."
]


class SimpleFatalOperation(BaseOperator):
    def finalize(self, context: dict):
        super().finalize(context=context)
        assert context.get('mabel:errored', False)
        return context
    def execute(self, data: dict, context: dict):
        super().execute(data=data, context=context)
        raise SystemExit("This was always doomed to failure")
    

class TriggerFatalOperation(BaseOperator):
    def execute(self, data: dict, context: dict):
        super().execute(data=data, context=context)
        raise SystemExit("This was always doomed to failure")
    
class DetectFatalOperation(BaseOperator):
    def finalize(self, context: dict):
        super().finalize(context=context)
        assert context.get('mabel:errored', False)
        return context
    def execute(self, data: dict, context: dict):
        super().execute(data=data, context=context)
        return data, context


def test_detect_failure_simple():
    """
    Run a basic flow
    """
    e = EndOperator()
    f = SimpleFatalOperation()
    flow = f >> e

    errored = False
    try:
        with flow as runner:
            for rule in FIGHT_CLUB_RULES:
                runner(data=rule)
    except SystemExit:  # pragma: no cover
        errored = True

    assert errored

def test_detect_failure_fork():
    """
    Run a basic flow
    """
    e = EndOperator()
    n1 = NoOpOperator()
    t = TriggerFatalOperation()
    d = DetectFatalOperation()
    flow = n1 >> [t >> e, d >> e]

    errored = False
    try:
        with flow as runner:
            for rule in FIGHT_CLUB_RULES:
                runner(data=rule)
    except SystemExit:  # pragma: no cover
        errored = True

    assert errored



if __name__ == "__main__":  # pragma: no cover

    test_detect_failure_simple()
    test_detect_failure_fork()

    print("okay")
