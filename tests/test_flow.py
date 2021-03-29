"""
Tests for the execution of flows. Create a basic flow
and push a payload through it.
"""
import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from mabel.flows import Flow
try:
    from rich import traceback
    traceback.install()
except ImportError:
    pass


def test_flow_operations():

    f = Flow()
    f.add_operator('p', print)
    f.add_operator('m', max)
    f.link_operators('p', 'm')

    assert len(f.nodes) == 2
    assert 'p' in f.nodes.keys()
    assert f.get_operator('p') == print
    assert len(f.edges) == 1
    assert f.get_entry_points() == ['p']
    assert f.get_outgoing_links('p') == ['m']

    g = Flow()
    g.add_operator('o', open)
    g.add_operator('n', None)
    g.link_operators('o', 'n')

    assert len(g.nodes) == 2
    assert 'n' in g.nodes.keys()
    assert g.get_operator('o') == open
    assert len(g.edges) == 1
    assert g.get_entry_points() == ['o']
    assert g.get_outgoing_links('n') == []

    f.merge(g)
    f.link_operators('n', 'p')

    assert len(f.nodes) == 4
    assert 'n' in f.nodes.keys()
    assert f.get_operator('o') == open
    assert len(f.edges) == 3
    assert f.get_entry_points() == ['o']
    assert f.get_outgoing_links('n') == ['p']


if __name__ == "__main__":
    test_flow_operations()

    print('okay')
