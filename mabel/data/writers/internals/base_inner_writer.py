"""
Writers are the target specific implementations which commit a temporary file
created by the BlobWriter to different systems, such as the filesystem,
Google Cloud Storage or MinIO.

The primary activity is contained in the .commit() method.
"""
import os
import abc
import uuid
import time
from functools import lru_cache
from ....utils import paths

STEM = "{stem}"


class BaseInnerWriter(abc.ABC):
    def __init__(self, **kwargs):

        dataset = kwargs.get("dataset")
        self.bucket, path, _, _ = paths.get_parts(dataset)

        if self.bucket == "/":
            self.bucket = ""
        if path == "/":
            path = ""

        self.extension = kwargs.get("extension", ".jsonl")
        if kwargs.get("format", "") in ["zstd", "parquet"]:
            self.extension = self.extension + "." + kwargs["format"]
        if kwargs.get("format") == "text":
            self.extension = ".txt"

        self.filename = self.bucket + "/" + path + STEM + self.extension
        self.filename_without_bucket = path + STEM + self.extension

    def _build_path(self):
        blob_id = f"{time.time_ns():x}-{self._get_node()}"
        return self.filename.replace(STEM, f"{blob_id}")

    @lru_cache(1)
    def _get_node(self):
        return f"{uuid.getnode():x}-{os.getpid():x}"

    @abc.abstractclassmethod
    def commit(self, byte_data, override_blob_name=None):
        pass
