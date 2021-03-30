import datetime
from ..internals.base_operator import BaseOperator
from ...data import BatchWriter
from ...adapters.minio import MinIoWriter
from ...data.validator import Schema  # type:ignore


class SaveSnapshotToMinIoOperator(BaseOperator):

    def __init__(
            self,
            *,
            end_point,
            access_key,
            secret_key,
            secure: bool = False,
            **kwargs):
        """
        """
        super().__init__()
        kwargs['end_point'] = end_point
        kwargs['access_key'] = access_key
        kwargs['secret_key'] = secret_key
        kwargs['secure'] = secure
        self.writer = BatchWriter(
                inner_writer=MinIoWriter,
                **kwargs)

    def execute(self, data: dict = {}, context: dict = {}):
        self.writer.append(data)
        return data, context

    def finalize(self):
        self.writer.finalize()

    def __del__(self):
        try:
            self.writer.finalize()
        except Exception:  # nosec - if this fails, it should be ignored here
            pass
