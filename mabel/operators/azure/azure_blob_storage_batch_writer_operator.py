from ...flows.internals.base_writer_operator import BaseWriterOperator
from ...data import BatchWriter
from ...adapters.azure import AzureBlobStorageWriter


class AzureBlobStorageBatchWriterOperator(BaseWriterOperator):
    def __init__(self, *, **kwargs):

        super().__init__(
            writer=BatchWriter, inner_writer=AzureBlobStorageWriter, **kwargs
        )
