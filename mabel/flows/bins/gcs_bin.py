"""
Google Cloud Storage Bin Writer
"""
import time
from .base_bin import BaseBin
from ...errors import MissingDependencyError

try:
    from google.cloud import storage  # type:ignore

    google_cloud_storage_installed = True
except ImportError:  # pragma: no cover
    google_cloud_storage_installed = False


class GoogleCloudStorageBin(BaseBin):
    def __init__(self, bin_name: str, project: str, bucket: str, path: str):

        if not google_cloud_storage_installed:  # pragma: no cover
            raise MissingDependencyError(
                "`google-cloud-storage` is missing, please install or include in requirements.txt"
            )

        client = storage.Client(project=project)
        self.bucket = client.get_bucket(bucket)
        self.path = path
        self.name = bin_name

    def __str__(self) -> str:
        return self.name

    def __call__(self, record: str, id_: str = ""):

        filename = f"{self.path}/{self._date_part()}/{id_}{time.time_ns()}.txt"
        blob = self.bucket.blob(filename)
        blob.upload_from_string(str(record).encode("utf-8"))
        return filename
