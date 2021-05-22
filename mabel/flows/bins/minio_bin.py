"""
MinIO Bin Writer

May support AWS S3 - untested
"""
import time
import io
from .base_bin import BaseBin
from ...errors import MissingDependencyError

try:
    from minio import Minio  # type:ignore

    minio_installed = True
except ImportError:  # pragma: no cover
    minio_installed = False


class MinioBin(BaseBin):
    def __init__(
        self,
        bin_name: str,
        end_point: str,
        bucket: str,
        path: str,
        access_key: str,
        secret_key: str,
        secure: bool = True,
    ):

        if not minio_installed:  # pragma: no cover
            raise MissingDependencyError(
                "`minio` is missing, please install or include in requirements.txt"
            )

        self.client = Minio(end_point, access_key, secret_key, secure=secure)
        self.bucket = bucket
        self.path = path
        self.name = bin_name

    def __str__(self) -> str:
        return self.name

    def __call__(self, record: str, id_: str = ""):

        filename = f"{self.path}/{self._date_part()}/{id_}{time.time_ns()}.txt"
        record_bytes = record.encode("utf-8")
        record_stream = io.BytesIO(record_bytes)

        self.client.put_object(
            self.bucket, filename, record_stream, length=len(record_bytes)
        )

        return filename
