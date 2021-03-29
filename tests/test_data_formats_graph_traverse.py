import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from mabel.data.formats.graphs import Graph
from data.graph_data import build_graph, graph_is_as_expected
from mabel.data.formats import graphs


def test_traversal():

    graph = build_graph()
    graph_is_as_expected(graph)

    d_1 = graphs.walk(graph, 'Lainie')

    # test the start is from the right point
    assert sorted(d_1.list_relationships()) == ['Likes', 'Lives In', 'Mother']
    assert d_1.active_nodes() == {'Lainie'}

    # test follow
    d_2 = d_1.follow('Mother')
    assert sorted(d_2.list_relationships()) == ['Daughter', 'Likes', 'Lives In', 'Sister']
    assert sorted(d_2.active_nodes()) == ['Ceanne', 'Sharlene']
    assert d_2.values('node_type') == ['Person']
    
    # test filtering
    d_3 = d_1.follow('Likes', 'Lives In', 'Mother')
    assert sorted(d_3.has('node_type', 'Person').active_nodes()) == ['Ceanne', 'Sharlene']
    assert d_3.has('node_type', 'Locality').active_nodes() == {'Toodyay'}
    assert d_3.select(lambda r: r['node_type'] == 'Restaurant').active_nodes() == {'Kailis Bros'}


if __name__ == "__main__":

    test_traversal()
    
    print('okay')
