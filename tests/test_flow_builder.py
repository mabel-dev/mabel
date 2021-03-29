"""
These for the flow builder, the flow builder is the code 
which build flows from Operators using the greater than
mathematical Operator (>). 
"""
import pytest
import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from mabel.logging import get_logger
from mabel.operators import FilterOperator, EndOperator, NoOpOperator
from mabel.flows import Flow
try:
    from rich import traceback
    traceback.install()
except ImportError:  # pragma: no cover
    pass


def test_flow_builder_valid():
    """
    Test the flow builder
    """
    e = EndOperator()
    f = FilterOperator()
    n = NoOpOperator()
    flow = f > n > e

    assert isinstance(flow, Flow)
    assert F"EndOperator-{id(e)}" in flow.nodes.keys()
    assert F"FilterOperator-{id(f)}" in flow.nodes.keys()
    assert F"NoOpOperator-{id(n)}" in flow.nodes.keys()
    assert len(flow.edges) == 2

    assert hasattr(flow, 'run')
    assert hasattr(flow, 'finalize')

    flow.finalize()


def test_flow_builder_invalid_uninstantiated():
    """
    Test the flow builder doesn't succeed with an invalid Operator
    """
    e = EndOperator      # <- this should fail as it's not initialized
    n = NoOpOperator()

    with pytest.raises(TypeError):
        flow = n > e
        flow.finalize()


def test_flow_builder_invalid_wrong_type():
    """
    Test the flow builder doesn't succeed with an invalid Operator
    """
    e = get_logger()      # <- this should fail as it's not an Operator
    n = NoOpOperator()

    with pytest.raises(TypeError):
        flow = n > e
        flow.finalize()

class TestOperator(NoOpOperator):
    def execute(self, data={}, context={}):
        print(data)
        return data, context

class OperatorA(TestOperator): pass
class OperatorB(TestOperator): pass
class OperatorC(TestOperator): pass
class OperatorD(TestOperator): pass

def test_branching():
    z = EndOperator()
    a = OperatorA()
    b = OperatorB()
    c = OperatorC()
    d = OperatorD()

    flow = a > [b > z, c > d > z]
    
    RESULTS = {
        'OperatorA': ['OperatorB', 'OperatorC'],
        'OperatorB': ['EndOperator'],
        'OperatorC': ['OperatorD'],
        'OperatorD': ['EndOperator']
    }

    for source, target in RESULTS.items():
        assert sorted([t.split('-')[0] for s, t in flow.edges if str(s).startswith(source)]) == target

    flow.run('>', {})
    flow.finalize()

    
if __name__ == "__main__":

    #test_flow_builder_valid()
    #test_flow_builder_invalid_uninstantiated()
    #test_flow_builder_invalid_wrong_type()
    test_branching()

    print('okay')
