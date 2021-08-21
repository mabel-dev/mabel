from ...flows.internals.base_writer_operator import BaseWriterOperator
from ...data import StreamWriter
from ...adapters.azure import AzureBlobStorageWriter


class AzureBlobStorageStreamWriterOperator(BaseWriterOperator):
    def __init__(self, *, **kwargs):

        super().__init__(
            writer=StreamWriter, inner_writer=AzureBlobStorageWriter, **kwargs
        )
