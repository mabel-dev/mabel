from ...flows.internals.base_writer_operator import BaseWriterOperator
from ...data import BatchWriter
from ...adapters.null import NullWriter


class NullBatchWriterOperator(BaseWriterOperator):
    def __init__(self, **kwargs):
        """ """
        super().__init__(writer=BatchWriter, inner_writer=NullWriter, **kwargs)
