import os
from ...data.writers.internals.base_inner_writer import BaseInnerWriter
from ...utils import paths
try:
    from google.auth.credentials import AnonymousCredentials  # type:ignore
    from google.cloud import storage  # type:ignore
except ImportError:
    pass


class GoogleCloudStorageWriter(BaseInnerWriter):

    def __init__(
            self,
            project: str,
            **kwargs):
        super().__init__(**kwargs)

        # this means we're testing
        if os.environ.get("STORAGE_EMULATOR_HOST") is not None:
            client = storage.Client(
                credentials=AnonymousCredentials(),
                project=project,
            )
        else:
            client = storage.Client(project=project)
        self.gcs_bucket = client.get_bucket(self.bucket)
        self.filename = self.filename_without_bucket

    def commit(
            self,
            byte_data,
            file_name=None):

        _filename = self._build_path()
        bucket, path, filename, ext = paths.get_parts(_filename)
        if file_name:
            _filename = bucket + '/' + path + '/' + file_name

        blob = self.gcs_bucket.blob(_filename)
        blob.upload_from_string(
                byte_data,
                content_type="application/octet-stream")
        return _filename
