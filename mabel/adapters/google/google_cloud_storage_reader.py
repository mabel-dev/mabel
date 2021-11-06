"""
Google Cloud Storage Reader
"""
import os
from ...data.readers.internals.base_inner_reader import BaseInnerReader
from ...errors import MissingDependencyError
from ...utils import paths

try:
    from google.auth.credentials import AnonymousCredentials  # type:ignore
    from google.cloud import storage  # type:ignore

    google_cloud_storage_installed = True
except ImportError:  # pragma: no cover
    google_cloud_storage_installed = False


class GoogleCloudStorageReader(BaseInnerReader):

    RULES = [{"name": "project", "required": False}]

    def __init__(self, project: str, credentials=None, **kwargs):
        if not google_cloud_storage_installed:  # pragma: no cover
            raise MissingDependencyError(
                "`google-cloud-storage` is missing, please install or include in requirements.txt"
            )

        super().__init__(**kwargs)
        self.project = project
        self.credentials = credentials

    def get_blob_bytes(self, blob_name):
        bucket, object_path, name, extension = paths.get_parts(blob_name)
        blob = get_blob(
            project=self.project,
            bucket=bucket,
            blob_name=object_path + name + extension,
        )
        stream = blob.download_as_bytes()
        return stream

    def get_blobs_at_path(self, path):
        bucket, object_path, name, extension = paths.get_parts(path)

        # this means we're testing
        if os.environ.get("STORAGE_EMULATOR_HOST") is not None:
            client = storage.Client(
                credentials=AnonymousCredentials(),
                project=self.project,
            )
        else:  # pragma: no cover
            client = storage.Client(project=self.project)

        gcs_bucket = client.get_bucket(bucket)
        blobs = list(client.list_blobs(bucket_or_name=gcs_bucket, prefix=object_path))

        yield from [
            bucket + "/" + blob.name for blob in blobs if not blob.name.endswith("/")
        ]


def get_blob(project: str, bucket: str, blob_name: str):

    # this means we're testing
    if os.environ.get("STORAGE_EMULATOR_HOST") is not None:
        client = storage.Client(
            credentials=AnonymousCredentials(),
            project=project,
        )
    else:  # pragma: no cover
        client = storage.Client(project=project)

    gcs_bucket = client.get_bucket(bucket)
    blob = gcs_bucket.get_blob(blob_name)
    return blob
