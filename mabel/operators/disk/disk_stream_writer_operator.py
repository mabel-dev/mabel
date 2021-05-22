from ...flows.internals.base_writer_operator import BaseWriterOperator
from ...data import StreamWriter  # type:ignore
from ...adapters.disk import DiskWriter


class DiskStreamWriterOperator(BaseWriterOperator):
    def __init__(self, **kwargs):

        super().__init__(writer=StreamWriter, inner_writer=DiskWriter, **kwargs)
