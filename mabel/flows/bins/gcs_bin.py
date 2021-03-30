"""
Google Cloud Storage Bin Writer
"""
try:
    from google.cloud import storage  # type:ignore
except ImportError:
    pass
import time
from .base_bin import BaseBin


class GoogleCloudStorageBin(BaseBin):

    def __init__(
            self,
            bin_name: str,
            project: str,
            bucket: str,
            path: str):
        client = storage.Client(project=project)
        self.bucket = client.get_bucket(bucket)
        self.path = path
        self.name = bin_name

    def __str__(self) -> str:
        return self.name

    def __call__(
            self,
            record: str,
            id_: str = ''):

        filename = F"{self.path}/{self._date_part()}/{id_}{time.time_ns()}.txt"
        blob = self.bucket.blob(filename)
        blob.upload_from_string(str(record).encode('utf-8'))
        return filename
