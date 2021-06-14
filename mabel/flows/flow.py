"""
A Flow is a simplified Graph, this originally used NetworkX but it was replaced
with a bespoke graph implementation as the NetworkX implementation was being
monkey-patches to make it easier to use. The decision was made to write a
specialized, albeit simple, graph library that didn't require monkey-patching.
"""
from typing import List
from .bins import FileBin, GoogleCloudStorageBin, MinioBin
from .flow_runner import FlowRunner
from .internals.base_operator import BaseOperator
from ..errors import FlowError


class Flow:
    def __init__(self):
        """
        Flow represents Directed Acyclic Graphs which are used to describe data
        pipelines.
        """
        self.nodes = {}
        self.edges = []
        self.has_run = False

    def add_operator(self, name, operator):
        """
        Add a step to the DAG

        Parameters:
            name: string
                The name of the step, must be unique
            Operator: BaseOperator
                The Operator
        """
        self.nodes[name] = operator

    def link_operators(self, source_operator, target_operator):
        """
        Link steps in a flow.

        Parameters:
            source_operator: string
                The name of the source step
            target_operator: string
                The name of the target step
        """
        edge = (source_operator, target_operator)
        if edge not in self.edges:
            self.edges.append((source_operator, target_operator))

    def get_outgoing_links(self, name):
        """
        Get the names of outgoing links from a given step.

        Paramters:
            name: string
                The name of the step to search from
        """
        retval = {target for source, target in self.edges if source == name}
        return sorted(retval)

    def get_exit_points(self):
        """
        Get steps in the flow with no outgoing steps.
        """
        sources = {source for source, target in self.edges}
        retval = {target for source, target in self.edges if target not in sources}
        return sorted(retval)

    def get_entry_points(self):
        """
        Get steps in the flow with no incoming steps.
        """
        targets = {target for source, target in self.edges}
        retval = {source for source, target in self.edges if source not in targets}
        return sorted(retval)

    def is_acyclic(self):
        # cycle over the graph removing a layer of exits each cycle
        # if we have nodes but no exists, we're cyclic
        my_edges = self.edges.copy()

        while len(my_edges) > 0:
            # find all of the exits
            sources = {source for source, target in my_edges}
            exits = {target for source, target in my_edges if target not in sources}

            if len(exits) == 0:
                return False

            # remove the exits
            new_edges = [
                (source, target) for source, target in my_edges if target not in exits
            ]
            my_edges = new_edges
        return True

    def get_operator(self, name):
        """
        Get the Operator class by name.

        Parameters:
            name: string
                The name of the step
        """
        return self.nodes.get(name)

    def merge(self, assimilatee):
        """
        Merge a flow into the current flow.

        Parameters:
            assimilatee: Flow
                The flow to assimilate into the current flows
        """
        self.nodes = {**self.nodes, **assimilatee.nodes}
        self.edges += assimilatee.edges
        self.edges = list(set(self.edges))

    def attach_writers(self, writers: List[dict]):

        for writer in writers:
            name = writer.get("name")
            class_name = writer.get("class")

            if class_name == "gcs":
                writer = GoogleCloudStorageBin(  # type: ignore
                    bin_name=name,  # type: ignore
                    project=writer.get("project"),  # type: ignore
                    bucket=writer.get("bucket"),  # type: ignore
                    path=writer.get("path"),  # type: ignore
                )
                self._attach_writer(writer)

            if class_name == "file" or class_name == "disk":
                writer = FileBin(  # type: ignore
                    bin_name=name, path=writer.get("path")  # type: ignore
                )  # type: ignore
                self._attach_writer(writer)

            if class_name == "minio":
                writer = MinioBin(  # type: ignore
                    bin_name=name,  # type: ignore
                    end_point=writer.get("end_point"),  # type: ignore
                    bucket=writer.get("bucket"),  # type: ignore
                    path=writer.get("path"),  # type: ignore
                    access_key=writer.get("access_key"),  # type: ignore
                    secret_key=writer.get("secret_key"),  # type: ignore
                    secure=writer.get("secure", True),
                )  # type: ignore
                self._attach_writer(writer)

    def _attach_writer(self, writer):
        """
        Attach the writer to each node in the flow
        """
        from ..logging import get_logger

        try:
            for operator_name in self.nodes:
                operator = self.get_operator(operator_name)
                setattr(operator, str(writer.name), writer)
            return True
        except Exception as err:
            get_logger().error(
                f"Failed to add writer to flow - {type(err).__name__} - {err}"
            )
            return False

    def _validate_flow(self):
        from ..operators import EndOperator

        # flow must be more than one item long
        if len(self.nodes) <= 1:
            raise FlowError(
                "Flow failed validation - Flows must have more than one Operator"
            )

        # flow paths must end with end operators
        if not all(
            [
                isinstance(self.get_operator(node), EndOperator)
                for node in self.get_exit_points()
            ]
        ):
            raise FlowError(
                "Flow failed validation - Flows must end with an EndOperator"
            )

        # flows must be acyclic
        if not self.is_acyclic():
            raise FlowError("Flow failed validation - Flows must be acyclic")

    def __enter__(self):
        if self.has_run:
            raise FlowError(
                "Flows can only have a single runner, either loop after creating the runner or build the flow again."
            )
        self._validate_flow()
        return FlowRunner(self)

    def __exit__(self, *args):
        """
        Finalize concludes the flow and returns the sensor information
        """
        from ..logging import get_logger

        FlowRunner(self)(BaseOperator.sigterm(), {})
        for operator_name in self.nodes.keys():
            operator = self.get_operator(operator_name)
            if operator:
                get_logger().audit(operator.read_sensors())
        self.has_run = True

    def __repr__(self):
        if not self.is_acyclic():
            return "Flow: cannot represent cyclic flows"
        return "\n".join(list(self._draw()))

    def __str__(self) -> str:
        return self.get_entry_points().pop()

    def _draw(self):
        for entry in self.get_entry_points():
            yield (f"{str(entry)}")
            t = self._tree(entry, "")
            yield ("\n".join(t))

    def _tree(self, node, prefix=""):

        space = "    "
        branch = " │  "
        tee = " ├─ "
        last = " └─ "

        contents = self.get_outgoing_links(node)
        # contents each get pointers that are ├── with a final └── :
        pointers = [tee] * (len(contents) - 1) + [last]
        for pointer, child_node in zip(pointers, contents):
            yield prefix + pointer + str(child_node)
            if len(self.get_outgoing_links(node)) > 0:
                # extend the prefix and recurse:
                extension = branch if pointer == tee else space
                # i.e. space because last, └── , above so no more |
                yield from self._tree(str(child_node), prefix=prefix + extension)
