import io
from mabel.data.writers.internals.base_inner_writer import BaseInnerWriter
from mabel.exceptions import MissingDependencyError

try:
    from minio import Minio  # type:ignore

    minio_installed = True
except ImportError:  # pragma: no cover
    minio_installed = False


class MinIoWriter(BaseInnerWriter):
    def __init__(
        self,
        *,
        end_point: str,
        access_key: str,
        secret_key: str,
        secure: bool = False,
        **kwargs
    ):

        if not minio_installed:  # pragma: no cover
            raise MissingDependencyError(
                "`minio` is missing, please install or include in requirements.txt"
            )
        super().__init__(**kwargs)

        self.client = Minio(end_point, access_key, secret_key, secure=secure)
        self.filename = self.filename_without_bucket

    def commit(self, byte_data, blob_name=None):

        self.client.put_object(
            self.bucket, blob_name, io.BytesIO(byte_data), len(byte_data)
        )

        return blob_name
