from ...flows.internals.base_writer_operator import BaseWriterOperator
from ...data import BatchWriter
from ...adapters.google import GoogleCloudStorageWriter


class GoogleStorageBatchWriterOperator(BaseWriterOperator):
    def __init__(self, *, project: str = None, **kwargs):

        kwargs["project"] = project
        super().__init__(
            writer=BatchWriter, inner_writer=GoogleCloudStorageWriter, **kwargs
        )
