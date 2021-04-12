from ..internals.base_writer_operator import BaseWriterOperator
from ...data import StreamWriter  # type:ignore
from ...adapters.local import FileWriter


class StreamToDiskOperator(BaseWriterOperator):

    def __init__(
            self,
            **kwargs):

        super().__init__(
                writer=StreamWriter,
                inner_writer=FileWriter,
                **kwargs)
