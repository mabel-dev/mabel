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
from typing import Callable


class Traverse:

    __slots__ = ("graph", "_active_nodes", "_active_nodes_cache")

    def __init__(self, graph, active_nodes: set = None):
        """
        Graph Traversal

        Parameters:
            graph: Graph
            active_nodes: Iterable
        """
        self.graph = graph
        self._active_nodes_cache = None

        # ensure it is a set
        # - the collection active nodes is immutable
        # - sets are faster for look ups
        if not active_nodes:
            self._active_nodes = set()
        elif type(active_nodes).__name__ == "set":
            self._active_nodes = active_nodes
        else:
            self._active_nodes = set(active_nodes)

    def follow(self, *relationships):
        """
        Traverses a graph by following edges from the active nodes with
        a relationship on the list of relationships.

        Parameters:
            relationsips: strings
                traverses node following edges with the stated relationship

        Returns:
            Diablo
        """
        active_nodes = []

        for node in self._active_nodes:
            active_nodes += [
                t for (s, t, r) in self.graph.outgoing_edges(node) if r in relationships
            ]
        return Traverse(graph=self.graph, active_nodes=active_nodes)

    def select(self, filter: Callable):
        """
        Filters the active nodes by a function.

        Parameters:
            filter: Callable
                node attribute name to filter on

        Returns:
            Diablo
        """
        active_nodes = {
            nid for nid, attrib in self.active_nodes(data=True) if filter(attrib)
        }
        return Traverse(graph=self.graph, active_nodes=active_nodes)

    def has(self, key, value):
        """
        Filters the active nodes by a key/value match
        """
        active_nodes = [
            nid
            for nid, attrib in self.active_nodes(data=True)
            if attrib.get(key) == value
        ]
        return Traverse(graph=self.graph, active_nodes=active_nodes)

    def values(self, key):
        return list(
            {
                attrib[key]
                for nid, attrib in self.active_nodes(data=True)
                if key in attrib
            }
        )

    def active_nodes(self, data=False):
        if not data:
            return self._active_nodes
        if not self._active_nodes_cache:
            self._active_nodes_cache = [
                (nid, self.graph[nid]) for nid in self._active_nodes
            ]
        return self._active_nodes_cache

    def list_relationships(self):
        relationships = []
        for node in self._active_nodes:
            relationships += {r for (s, t, r) in self.graph.outgoing_edges(node)}
        return set(relationships)

    def __repr__(self):
        return f"Graph - {len(list(self.graph.nodes()))} nodes ({len(self._active_nodes)} selected), {len(list(self.graph.edges()))} edges"

    def __len__(self):
        return len(self._active_nodes)
