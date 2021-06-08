"""
Diablo: Python Graph Library

(C) 2021 Justin Joyce.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

from pathlib import Path
from typing import Iterable, Tuple
from ....errors import MissingDependencyError


class Graph(object):
    """
    Graph object, optimized for traversal.

    Edges are stored in a dictionary, the key is the source node to speed up
    finding outgoing edges. The Edges only have three pieces of data:
        - the source node (the key)
        - the target node
        - the relationship
    The target and the relationship are stored as a tuple, the edge dictionary
    stores lists of tuples.

    Nodes are stored as a B+Tree, this gives slightly slower performance
    than a dictionary but has a distinct advantage in that it sorts the
    values enabling binary searching of the dataset without loading into
    memory.
    """

    __slots__ = ("_nodes", "_edges")

    def __init__(self):
        """
        Directed Graph.
        """
        self._nodes = {}
        self._edges = {}

    def _make_a_list(self, obj):
        """internal helper method"""
        if isinstance(obj, list):
            return obj
        return [obj]

    def save(self, graph_path):
        """
        Persist a graph to storage. It saves nodes and edges to separate files.

        Parameters:
            graph_path: string
                The folder ?to save the node and edge files to
        """
        path = Path(graph_path)
        path.mkdir(exist_ok=True)

        from .internals import EdgeModel, NodeModel

        with open(path / "edges.jsonl", "w", encoding="utf8") as edge_file:
            for source, target, relationship in self.edges():
                edge_record = EdgeModel(
                    source=source,
                    target=target,
                    relationship=relationship,
                )
                edge_file.write(edge_record.json() + "\n")
        with open(path / "nodes.jsonl", "w", encoding="utf8") as node_file:
            for nid, attributes in self.nodes(data=True):
                node_record = NodeModel(nid=nid, attributes=attributes)
                node_file.write(node_record.json() + "\n")

    def add_edge(self, source: str, target: str, relationship: str):
        """
        Add edge to the graph

        Note:
            This does not create nodes if they don't already exist

        Parameters:
            source: string
                The source node
            target: string
                The target node
            relationship: string
                The relationship between the source and target nodes
        """
        if source not in self._edges:
            targets = []
        else:
            targets = self._edges[source]
        targets.append(
            (
                target,
                relationship,
            )
        )
        self._edges[source] = list(set(targets))

    def add_node(self, nid: str, attributes: dict = {}):
        """
        Add node to the graph

        Parameters:
            nid: string
                The Node ID for the node (unique)
            attributes: dictionary (optional)
                The attributes of the node
        """
        self._nodes[nid] = attributes

    def nodes(self, data=False):
        """
        The nodes which comprise the graph

        Parameters:
            data: boolean (optional)
                if True return the details of the nodes, if False just return
                the list of node IDs

        Returns:
            List
        """
        if data:
            return self._nodes.items()
        return list(self._nodes.keys())

    def edges(self):
        """
        The edges which comprise the graph

        Returns:
            Generator of Tuples of (Source, Target and Relationship)
        """
        for s, records in self._edges.items():
            for t, r in records:
                yield s, t, r

    def breadth_first_search(self, source: str, depth: int = 100):
        """
        Search a tree for nodes we can walk to from a given node.

        Parameters:
            source: string
                The node to walk from
            depth: integer
                The maximum distance to walk from source
        Returns:
        """
        # This uses a variation of the algorith used by NetworkX optimized for
        # the Diablo data structures.
        #
        # https://networkx.org/documentation/networkx-1.10/_modules/networkx/algorithms/traversal/breadth_first_search.html#bfs_tree

        from collections import deque

        distance = 0

        visited = set([source])
        queue = deque(
            [
                (
                    source,
                    distance,
                    self.outgoing_edges(source),
                )
            ]
        )

        new_edges = []

        while queue:
            parent, node_distance, children = queue[0]
            if node_distance < depth:
                for child in children:
                    s, t, r = child
                    new_edges.append(child)
                    if t not in visited:
                        visited.add(t)
                        queue.append(
                            (
                                t,
                                node_distance + 1,
                                self.outgoing_edges(t),
                            )
                        )
            queue.popleft()
        return new_edges

    def outgoing_edges(self, source) -> Iterable[Tuple]:
        """
        Get the list of edges traversable from a given node.

        Parameters:
            source: string
                The node to get the outgoing edges for

        Returns:
            Set of Tuples (Source, Target, Relationship)
        """
        targets = self._edges.get(source) or {}
        return {(source, t, r) for t, r in targets}

    def ingoing_edges(self, target) -> Iterable[Tuple]:
        """
        Get the list of edges which can traverse to a given node.

        Parameters:
            target: string
                The node to get the incoming edges for

        Returns:
            Set of Tuples (Source, Target, Relationship)
        """
        return [(s, t, r) for s, t, r in self.edges() if t == target]

    def copy(self):
        g = Graph()
        g._nodes = self._nodes.copy()
        g._edges = self._edges.copy()
        return g

    def to_networkx(self):
        """
        Convert a Diablo graph to a NetworkX graph
        """
        try:
            import networkx as nx  # type:ignore
        except ImportError:  # pragma: no cover
            raise MissingDependencyError(
                "`networx` is missing, please install or include in requirements.txt"
            )

        g = nx.DiGraph()
        for s, t, r in self.edges():
            g.add_edge(s, t, relationship=r)
        for node, attribs in self.nodes(True):
            g.add_node(node, **attribs)
        return g

    def epitomize(self):
        """
        Summarize a Graph by reducing to only the node_types and relationships
        """
        g = Graph()
        for s, t, r in self.edges():
            node1 = self[s]
            node2 = self[t]
            if node1 and node2:
                g.add_edge(node1.get("node_type"), node2.get("node_type"), r)
            if node1:
                g.add_node(
                    node1.get("node_type"), {"node_type": node1.get("node_type")}
                )
            if node2:
                g.add_node(
                    node2.get("node_type"), {"node_type": node2.get("node_type")}
                )
        return g

    def __repr__(self):
        return (
            f"Graph - {len(list(self.nodes()))} nodes, {len(list(self.edges()))} edges"
        )

    def __len__(self):
        return len(list(self.nodes()))

    def __getitem__(self, nid):
        return self._nodes.get(nid, {})
