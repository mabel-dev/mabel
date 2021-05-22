import os
from ...data.writers.internals.base_inner_writer import BaseInnerWriter
from ...errors import MissingDependencyError

try:
    from google.auth.credentials import AnonymousCredentials  # type:ignore
    from google.cloud import storage  # type:ignore

    google_cloud_storage_installed = True
except ImportError:  # pragma: no cover
    google_cloud_storage_installed = False


class GoogleCloudStorageWriter(BaseInnerWriter):
    def __init__(self, project: str, **kwargs):
        if not google_cloud_storage_installed:  # pragma: no cover
            raise MissingDependencyError(
                "`google-cloud-storage` is missing, please install or include in requirements.txt"
            )

        super().__init__(**kwargs)

        # this means we're testing
        if os.environ.get("STORAGE_EMULATOR_HOST") is not None:
            client = storage.Client(
                credentials=AnonymousCredentials(),
                project=project,
            )
        else:  # pragma: no cover
            client = storage.Client(project=project)
        self.gcs_bucket = client.get_bucket(self.bucket)
        self.filename = self.filename_without_bucket

    def commit(self, byte_data, override_blob_name=None):

        # if we've been given the filename, use that, otherwise get the
        # name from the path builder
        if override_blob_name:
            blob_name = override_blob_name
        else:
            blob_name = self._build_path()

        blob = self.gcs_bucket.blob(blob_name)
        blob.upload_from_string(byte_data, content_type="application/octet-stream")
        return blob_name
