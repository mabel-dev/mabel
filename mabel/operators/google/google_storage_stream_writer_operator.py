from ...flows.internals.base_writer_operator import BaseWriterOperator
from ...data import StreamWriter
from ...adapters.google import GoogleCloudStorageWriter


class GoogleStorageStreamWriterOperator(BaseWriterOperator):
    def __init__(self, *, project: str = None, **kwargs):

        kwargs["project"] = project
        super().__init__(
            writer=StreamWriter, inner_writer=GoogleCloudStorageWriter, **kwargs
        )
