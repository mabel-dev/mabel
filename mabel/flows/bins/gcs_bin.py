"""
Google Cloud Storage Bin Writer
"""
import os
import time
from .base_bin import BaseBin
from mabel.errors import MissingDependencyError
from urllib3.exceptions import ProtocolError  # type:ignore

try:
    from google.cloud import storage  # type:ignore
    from google.auth.credentials import AnonymousCredentials  # type:ignore
    from google.api_core import retry  # type:ignore
    from google.api_core.exceptions import (
        InternalServerError,
        TooManyRequests,
    )  # type:ignore

    google_cloud_storage_installed = True
except ImportError:  # pragma: no cover
    google_cloud_storage_installed = False


class GoogleCloudStorageBin(BaseBin):
    def __init__(self, bin_name: str, project: str, bucket: str, path: str):

        if not google_cloud_storage_installed:  # pragma: no cover
            raise MissingDependencyError(
                "`google-cloud-storage` is missing, please install or include in requirements.txt"
            )

        # this means we're testing
        if os.environ.get("STORAGE_EMULATOR_HOST") is not None:
            client = storage.Client(
                credentials=AnonymousCredentials(),
                project=project,
            )
        else:  # pragma: no cover
            client = storage.Client(project=project)

        self.bucket = client.get_bucket(bucket)
        self.path = path
        self.name = bin_name

    def __str__(self) -> str:
        return self.name

    def __call__(self, record: str, id_: str = ""):

        predicate = retry.if_exception_type(
            ConnectionResetError, ProtocolError, InternalServerError, TooManyRequests
        )
        self.retry = retry.Retry(predicate)

        filename = f"{self.path}/{self._date_part()}/{id_}{time.time_ns()}.txt"
        blob = self.bucket.blob(filename)
        self.retry(blob.upload_from_string)(
            str(record).encode("utf-8"), content_type="application/octet-stream"
        )
        return filename
