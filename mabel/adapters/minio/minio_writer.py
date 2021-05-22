import io
from ...data.writers.internals.base_inner_writer import BaseInnerWriter
from ...errors import MissingDependencyError

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

    def commit(self, byte_data, override_blob_name=None):

        # if we've been given the filename, use that, otherwise get the
        # name from the path builder
        if override_blob_name:
            blob_name = override_blob_name
        else:
            blob_name = self._build_path()

        self.client.put_object(
            self.bucket, blob_name, io.BytesIO(byte_data), len(byte_data)
        )

        return blob_name
