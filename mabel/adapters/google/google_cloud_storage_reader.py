"""
Google Cloud Storage Reader
"""
import io
import os
from ...data.readers.internals.base_inner_reader import BaseInnerReader
from ...utils import paths


class GoogleStorageReadError(Exception): pass

class GoogleCloudStorageReader(BaseInnerReader):

    RULES = [
        {"name": "project", "required": False},
        {"name": "credentials", "required": False},
    ]

    def __init__(self, project: str, credentials=None, **kwargs):
        super().__init__(**kwargs)
        self.project = project
        self.credentials = credentials

    def get_blob_stream(self, blob_name):
        bucket, object_path, name, extension = paths.get_parts(blob_name)
        blob = get_blob(
            project=self.project,
            bucket=bucket,
            blob_name=object_path + name + extension,
            credentials=self.credentials,
        )
        return blob

    def get_blob_chunk(self, blob_name: str, start: int, buffer_size: int) -> bytes:
        bucket, object_path, name, extension = paths.get_parts(blob_name)
        blob = get_blob(
            project=self.project,
            bucket=bucket,
            blob_name=object_path + name + extension,
            credentials=self.credentials,
        )
        stream = blob.download_as_bytes(
            start=start, end=min(blob.size, start + buffer_size - 1)
        )
        return stream

    def get_blobs_at_path(self, path):
        bucket, object_path, name, extension = paths.get_parts(path)

        import requests

        # determin the domain
        domain = os.environ.get("STORAGE_EMULATOR_HOST", "https://storage.googleapis.com")
        if domain[-1] != "/":
            domain += "/"

        # add the headers if needed
        headers = {}
        if self.credentials:
            headers["Authorization"] = f"Bearer {self.credentials}"

        # get the data
        payload = requests.get(
            url=f"{domain}storage/v1/b/{bucket}/o?prefix={object_path}",
            headers=headers,
            timeout=30
        )

        print(payload.content)

        if payload.status_code // 100 != 2:
            return []

        yield from [
            bucket + "/" + blob["name"] for blob in payload.json()["items"] if not blob["name"].endswith("/")
        ]


def get_blob(project: str, bucket: str, blob_name: str, credentials=None):

    import requests

    # determin the domain
    domain = os.environ.get("STORAGE_EMULATOR_HOST", "https://storage.googleapis.com")
    if domain[-1] != "/":
        domain += "/"

    # add the headers if needed
    headers = {}
    if credentials:
        headers["Authorization"] = f"Bearer {credentials}"

    # get the data
    payload = requests.get(
        url=f"{domain}storage/v1/b/{bucket}/o/{blob_name}?alt=media",
        headers=headers,
        timeout=30
    )

    print(payload.content)

    if payload.status_code // 100 != 2:
        raise GoogleStorageReadError(payload.content)

    return io.BytesIO(payload.content)
