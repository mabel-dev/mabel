import os
from urllib3.exceptions import ProtocolError  # type:ignore
from mabel.logging.create_logger import get_logger
from mabel.data.writers.internals.base_inner_writer import BaseInnerWriter
from mabel.exceptions import MissingDependencyError

try:
    from google.auth.credentials import AnonymousCredentials  # type:ignore
    from google.cloud import storage  # type:ignore
    from google.api_core import retry  # type:ignore
    from google.api_core.exceptions import (
        InternalServerError,
        TooManyRequests,
    )  # type:ignore

    google_cloud_storage_installed = True
except ImportError:  # pragma: no cover
    google_cloud_storage_installed = False


class GoogleCloudStorageWriter(BaseInnerWriter):
    def __init__(self, project: str, credentials=None, **kwargs):
        if not google_cloud_storage_installed:  # pragma: no cover
            raise MissingDependencyError(
                "`google-cloud-storage` is missing, please install or include in requirements.txt"
            )

        super().__init__(**kwargs)
        self.project = project
        self.credentials = credentials
        self.filename = self.filename_without_bucket

        predicate = retry.if_exception_type(
            ConnectionResetError, ProtocolError, InternalServerError, TooManyRequests
        )
        self.retry = retry.Retry(predicate)

    def commit(self, byte_data, blob_name=None):

        # this means we're testing
        if os.environ.get("STORAGE_EMULATOR_HOST") is not None:
            client = storage.Client(
                credentials=AnonymousCredentials(),
                project=self.project,
            )
        else:  # pragma: no cover
            client = storage.Client(project=self.project, credentials=self.credentials)
        self.gcs_bucket = client.get_bucket(self.bucket)

        try:
            blob = self.gcs_bucket.blob(blob_name)
            self.retry(blob.upload_from_string)(
                byte_data, content_type="application/octet-stream"
            )
            return blob_name
        except Exception as err:
            import traceback

            logger = get_logger()
            logger.error(
                f"Error Saving Blob to GCS {type(err).__name__} - {err}\n{traceback.format_exc()}"
            )
            raise err
