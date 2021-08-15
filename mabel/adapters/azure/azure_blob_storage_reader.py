"""
Azure Storage Reader
"""
import io
import os
from ...data.readers.internals.base_inner_reader import BaseInnerReader
from ...errors import MissingDependencyError
from ...utils import paths

try:
    from azure.storage.blob import BlobServiceClient

    azure_blob_storage_installed = True
except ImportError:  # pragma: no cover
    azure_blob_storage_installed = False


class AzureBlobStorageReader(BaseInnerReader):

    RULES = [{"name": "project", "required": False}]

    def __init__(self, project: str, **kwargs):
        if not azure_blob_storage_installed:  # pragma: no cover
            raise MissingDependencyError(
                "`azure-storage-blob` is missing, please install or include in requirements.txt"
            )

        super().__init__(**kwargs)
        self.project = project

    def get_blob_stream(self, object_name):
        bucket, object_path, name, extension = paths.get_parts(object_name)
        blob = get_blob(
            project=self.project,
            bucket=bucket,
            blob_name=object_path + name + extension,
        )
        stream = blob.download_as_bytes()
        io_stream = io.BytesIO(stream)
        return io_stream

    def get_blobs_at_path(self, path):
        pass

