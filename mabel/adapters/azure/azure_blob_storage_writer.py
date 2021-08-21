import os
from ...data.writers.internals.base_inner_writer import BaseInnerWriter
from ...errors import MissingDependencyError

try:
    from azure.storage.blob import BlobServiceClient

    azure_blob_storage_installed = True
except ImportError:  # pragma: no cover
    azure_blob_storage_installed = False


class AzureBlobStorageWriter(BaseInnerWriter):
    def __init__(self, **kwargs):
        if not azure_blob_storage_installed:  # pragma: no cover
            raise MissingDependencyError(
                "`azure-storage-blob` is missing, please install or include in requirements.txt"
            )
        try:
            os.environ["AZURE_STORAGE_CONNECTION_STRING"]
        except KeyError:
            raise ValueError(
                "Environment Variable `AZURE_STORAGE_CONNECTION_STRING` must be set."
            )

        super().__init__(**kwargs)

        blob_service_client = BlobServiceClient.from_connection_string(
            os.environ["AZURE_STORAGE_CONNECTION_STRING"]
        )
        self.container_client = blob_service_client.get_container_client(self.bucket)
        self.filename = self.filename_without_bucket

    def commit(self, byte_data, override_blob_name=None):

        # if we've been given the filename, use that, otherwise get the
        # name from the path builder
        if override_blob_name:
            blob_name = override_blob_name
        else:
            blob_name = self._build_path()

        blob_client = self.container_client.get_blob_client(blob_name)
        blob_client.upload_blob(byte_data, blob_type="BlockBlob")
        return blob_name
