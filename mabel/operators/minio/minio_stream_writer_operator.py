from ...flows.internals.base_writer_operator import BaseWriterOperator
from ...data import StreamWriter
from ...adapters.minio import MinIoWriter


class MinIoStreamWriterOperator(BaseWriterOperator):
    def __init__(
        self, *, end_point, access_key, secret_key, secure: bool = False, **kwargs
    ):
        """
        Streaming Write Operator for MinIo.

        Uses StreamWriter for logic and MinIoWriter for IO
        """
        kwargs["end_point"] = end_point
        kwargs["access_key"] = access_key
        kwargs["secret_key"] = secret_key
        kwargs["secure"] = secure

        super().__init__(writer=StreamWriter, inner_writer=MinIoWriter, **kwargs)
