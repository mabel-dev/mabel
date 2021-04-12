from ..internals.base_writer_operator import BaseWriterOperator
from ...data import BatchWriter
from ...adapters.local import FileWriter


class DiskBatchWriterOperator(BaseWriterOperator):

    def __init__(
            self,
            **kwargs):

        super().__init__(
                writer=BatchWriter,
                inner_writer=FileWriter,
                **kwargs)
