"""
These for the flow builder, the flow builder is the code 
which build flows from Operators using the greater than
mathematical Operator (>). 
"""
import pytest
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.logging import get_logger
from mabel.operators import FilterOperator, EndOperator, NoOpOperator
from mabel.flows import Flow
from rich import traceback

traceback.install()


def test_flow_builder_valid():
    """
    Test the flow builder
    """
    e = EndOperator()
    f = FilterOperator()
    n = NoOpOperator()
    flow = f >> n >> e

    assert isinstance(flow, Flow)
    assert f"EndOperator-{id(e)}" in flow.nodes.keys()
    assert f"FilterOperator-{id(f)}" in flow.nodes.keys()
    assert f"NoOpOperator-{id(n)}" in flow.nodes.keys()
    assert len(flow.edges) == 2


def test_flow_builder_invalid_uninstantiated():
    """
    Test the flow builder doesn't succeed with an invalid Operator
    """
    e = EndOperator  # <- this should fail as it's not initialized
    n = NoOpOperator()

    with pytest.raises(TypeError):
        flow = n >> e


def test_flow_builder_invalid_wrong_type():
    """
    Test the flow builder doesn't succeed with an invalid Operator
    """
    e = get_logger()  # <- this should fail as it's not an Operator
    n = NoOpOperator()

    with pytest.raises(TypeError):
        flow = n >> e


class TestOperator(NoOpOperator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def execute(self, data={}, context={}):
        return data, context


class OperatorA(TestOperator):
    pass


class OperatorB(TestOperator):
    pass


class OperatorC(TestOperator):
    pass


class OperatorD(TestOperator):
    pass


def test_branching():
    z = EndOperator()
    a = OperatorA()
    b = OperatorB()
    c = OperatorC()
    d = OperatorD()

    flow = a >> [b >> z, c >> d >> z]

    print(repr(flow))
    print(flow.edges)

    RESULTS = {
        "OperatorA": ["OperatorB", "OperatorC"],
        "OperatorB": ["EndOperator"],
        "OperatorC": ["OperatorD"],
        "OperatorD": ["EndOperator"],
    }

    for source, target in RESULTS.items():
        assert (
            sorted(
                [t.split("-")[0] for s, t in flow.edges if str(s).startswith(source)]
            )
            == target
        ), f"{source} - {target} - {sorted([t.split('-')[0] for s, t in flow.edges if str(s).startswith(source)])}"

    with flow as runner:
        runner(">", {})


def test_context_manager():
    z = EndOperator()
    a = OperatorA()
    b = OperatorB()
    c = OperatorC()
    d = OperatorD()

    flow = a >> [b >> z, c >> d >> z]

    RESULTS = {
        "OperatorA": ["OperatorB", "OperatorC"],
        "OperatorB": ["EndOperator"],
        "OperatorC": ["OperatorD"],
        "OperatorD": ["EndOperator"],
    }

    for source, target in RESULTS.items():
        assert (
            sorted(
                [t.split("-")[0] for s, t in flow.edges if str(s).startswith(source)]
            )
            == target
        )

    payloads = ["a", "b", "c", "d", "e"]
    with flow as runner:
        for payload in payloads:
            runner(payload, {})


if __name__ == "__main__":  # pragma: no cover

    test_flow_builder_valid()
    test_flow_builder_invalid_uninstantiated()
    test_flow_builder_invalid_wrong_type()
    test_branching()
    test_context_manager()

    print("okay")
