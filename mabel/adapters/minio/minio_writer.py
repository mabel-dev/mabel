import os
from ...data.writers.internals.base_inner_writer import BaseInnerWriter
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
            source_file_name):

        _filename = self._build_path()

        # put the file using the MinIO API
        with open(source_file_name, 'rb') as file_data:
            file_stat = os.stat(source_file_name)
            self.client.put_object(
                    self.bucket,
                    _filename,
                    file_data,
                    file_stat.st_size)

        return _filename
