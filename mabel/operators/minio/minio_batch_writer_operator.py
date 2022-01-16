from ...flows.internals.base_writer_operator import BaseWriterOperator
from ...data import BatchWriter
from ...adapters.minio import MinIoWriter


class MinIoBatchWriterOperator(BaseWriterOperator):
    def __init__(
        self, *, end_point, access_key, secret_key, secure: bool = False, **kwargs
    ):
        """ """
        kwargs["end_point"] = end_point
        kwargs["access_key"] = access_key
        kwargs["secret_key"] = secret_key
        kwargs["secure"] = secure

        super().__init__(writer=BatchWriter, inner_writer=MinIoWriter, **kwargs)
