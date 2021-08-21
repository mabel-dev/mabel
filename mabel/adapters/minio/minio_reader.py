"""
MinIo Reader - also works with AWS
"""
from functools import lru_cache
import io
from ...data.readers.internals.base_inner_reader import BaseInnerReader
from ...utils import paths, common
from ...errors import MissingDependencyError

try:
    from minio import Minio  # type:ignore

    minio_installed = True
except ImportError:  # pragma: no cover
    minio_installed = False


class MinIoReader(BaseInnerReader):

    RULES = [
        {"name": "end_point", "required": True},
        {"name": "access_key", "required": True},
        {"name": "secret_key", "required": True},
        {"name": "secure", "required": True},
    ]

    def __init__(self, end_point: str, access_key: str, secret_key: str, **kwargs):

        if not minio_installed:  # pragma: no cover
            raise MissingDependencyError(
                "`minio` is missing, please install or include in requirements.txt"
            )

        super().__init__(**kwargs)
        secure = kwargs.get("secure", True)
        self.minio = Minio(end_point, access_key, secret_key, secure=secure)

    def get_blobs_at_path(self, path):
        bucket, object_path, _, _ = paths.get_parts(path)
        for cycle_date in common.date_range(self.start_date, self.end_date):
            cycle_path = paths.build_path(path=object_path, date=cycle_date)
            blobs = self.minio.list_objects(
                bucket_name=bucket, prefix=cycle_path, recursive=True
            )

            yield from [
                bucket + "/" + blob.object_name
                for blob in blobs
                if not blob.object_name.endswith("/")
            ]

    def get_blob_stream(self, blob_name: str) -> io.IOBase:
        try:
            bucket, object_path, name, extension = paths.get_parts(blob_name)
            stream = self.minio.get_object(bucket, object_path + name + extension)

            io_stream = io.BytesIO(stream.read())
            return io_stream
        finally:
            stream.close()

    @lru_cache(4)
    def get_object_size(self, bucket_name, blob_name):
        try:
            return self.minio.stat_object(bucket_name, blob_name).size
        except:
            return 0

    def get_blob_chunk(self, blob_name: str, start: int, buffer_size: int) -> bytes:
        try:
            bucket, object_path, name, extension = paths.get_parts(blob_name)
            blob_path = object_path + name + extension
            size = self.get_object_size(bucket, blob_path) - start
            if size == 0:
                return bytes()
            stream = self.minio.get_object(
                bucket, blob_path, offset=start, length=min(buffer_size, size)
            )
            return stream.read()
        finally:
            pass  # stream.close()
