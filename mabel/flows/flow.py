"""
A Flow is a simplified Graph, this originally used NetworkX but it was replaced
with a bespoke graph implementation as the NetworkX implementation was being
monkey-patches to make it easier to use. The decision was made to write a
specialized, albeit simple, graph library that didn't require monkey-patching.
"""
from typing import Union, List
from .bins import FileBin, GoogleCloudStorageBin, MinioBin
from .flow_runner import FlowRunner
from .internals.base_operator import BaseOperator


class Flow():

    def __init__(self):
        """
        Flow represents Directed Acyclic Graphs which are used to describe data
        pipelines.
        """
        self.nodes = {}
        self.edges = []

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
        self.edges.append((source_operator, target_operator))

    def get_outgoing_links(self, name):
        """
        Get the names of outgoing links from a given step.

        Paramters:
            name: string
                The name of the step to search from
        """
        return [target for source,target in self.edges if source == name]

    def get_entry_points(self):
        """
        Get steps in the flow with no incoming steps.
        """
        targets = {target for source,target in self.edges}
        return [k for k,v in self.nodes.items() if k not in targets]

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

    def attach_writers(self, writers: List[dict]):

        for writer in writers:
            name = writer.get('name')
            class_name = writer.get('class')

            if class_name == 'gcs':
                writer = GoogleCloudStorageBin(                 # type: ignore
                        bin_name=name,                          # type: ignore
                        project=writer.get('project'),          # type: ignore
                        bucket=writer.get('bucket'),            # type: ignore
                        path=writer.get('path'))                # type: ignore
                self._attach_writer(writer)

            if class_name == 'file':
                writer = FileBin(                               # type: ignore
                        bin_name=name,                          # type: ignore
                        path=writer.get('path'))                # type: ignore
                self._attach_writer(writer)

            if class_name == 'minio':
                writer = MinioBin(                              # type: ignore
                        bin_name=name,                          # type: ignore
                        end_point=writer.get('end_point'),      # type: ignore
                        bucket=writer.get('bucket'),            # type: ignore
                        path=writer.get('path'),                # type: ignore
                        access_key=writer.get('access_key'),    # type: ignore
                        secret_key=writer.get('secret_key'),    # type: ignore
                        secure=writer.get('secure', True))      # type: ignore
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
            get_logger().error(F"Failed to add writer to flow - {type(err).__name__} - {err}")
            return False

    def __enter__(self):
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
