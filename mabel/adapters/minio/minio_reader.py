"""
MinIo Reader - may work with AWS
"""
import io
from ...utils import paths, common
from ...data.readers.internals.base_inner_reader import BaseInnerReader
try:
    from minio import Minio  # type:ignore
except ImportError:
    pass


class MinIoReader(BaseInnerReader):

    def __init__(
            self,
            end_point: str,
            access_key: str,
            secret_key: str,
            **kwargs):
        super().__init__(**kwargs)
        secure = kwargs.get('secure', True)
        self.minio = Minio(end_point, access_key, secret_key, secure=secure)


    def get_blobs_at_path(self, path):

        bucket, object_path, _, _ = paths.get_parts(path)
        for cycle_date in common.date_range(self.start_date, self.end_date):
            cycle_path = paths.build_path(path=object_path, date=cycle_date)
            objects = self.minio.list_objects(
                    bucket_name=bucket,
                    prefix=cycle_path,
                    recursive=True)
            for obj in objects:
                yield bucket + '/' + obj.object_name


    def get_blob_stream(self, blob_name:str) -> io.IOBase:
        bucket, object_path, name, extension = paths.get_parts(blob_name)
        stream = self.minio.get_object(bucket, object_path + name + extension).read()

        io_stream = io.BytesIO(stream)
        return io_stream
