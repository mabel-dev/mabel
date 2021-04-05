import os
import io
from ...data.writers.internals.base_inner_writer import BaseInnerWriter
from ...utils import paths
try:
    from minio import Minio  # type:ignore
except ImportError:
    pass


class MinIoWriter(BaseInnerWriter):

    def __init__(
            self,
            *,
            end_point: str,
            access_key: str,
            secret_key: str,
            secure: bool = False,
            **kwargs):
        super().__init__(**kwargs)

        self.client = Minio(end_point, access_key, secret_key, secure=secure)
        self.filename = self.filename_without_bucket

    def commit(
            self,
            byte_data,
            file_name=None):

        _filename = self._build_path()
        bucket, path, filename, ext = paths.get_parts(_filename)
        if file_name:
            _filename = bucket + '/' + path + '/' + file_name

        self.client.put_object(
                    self.bucket,
                    _filename,
                    io.BytesIO(byte_data),
                    len(byte_data))

        return _filename
