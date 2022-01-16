from ...flows.internals.base_writer_operator import BaseWriterOperator
from ...data import BatchWriter
from ...adapters.disk import DiskWriter


class DiskBatchWriterOperator(BaseWriterOperator):
    def __init__(self, **kwargs):

        super().__init__(writer=BatchWriter, inner_writer=DiskWriter, **kwargs)
