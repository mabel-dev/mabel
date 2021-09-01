"""
Azure Storage Reader
"""
import io
import os
from typing import List, Dict
from ...data.readers.internals.base_inner_reader import BaseInnerReader
from ...errors import MissingDependencyError
from ...utils import paths

try:
    from azure.storage.blob import BlobServiceClient

    azure_blob_storage_installed = True
except ImportError:  # pragma: no cover
    azure_blob_storage_installed = False


class AzureBlobStorageReader(BaseInnerReader):

    RULES: List[Dict] = []

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

        self.blob_service_client = BlobServiceClient.from_connection_string(
            os.environ["AZURE_STORAGE_CONNECTION_STRING"]
        )

    def get_blob_stream(self, blob_name):
        container, object_path, name, extension = paths.get_parts(blob_name)
        container_client = self.blob_service_client.get_container_client(container)
        blob = container_client.get_blob_client(object_path + name + extension)
        stream = blob.download_blob().readall()
        io_stream = io.BytesIO(stream)
        return io_stream

    def get_blob_chunk(self, blob_name: str, start: int, buffer_size: int) -> bytes:
        container, object_path, name, extension = paths.get_parts(blob_name)
        container_client = self.blob_service_client.get_container_client(container)
        blob = container_client.get_blob_client(object_path + name + extension)
        return blob.download_blob(offset=start, length=buffer_size).readall()

    def get_blobs_at_path(self, path):

        container, object_path, name, extension = paths.get_parts(path)
        container_client = self.blob_service_client.get_container_client(container)
        blobs = container_client.list_blobs(name_starts_with=object_path)

        blobs = list([container + "/" + b.name for b in blobs])
        yield from blobs
