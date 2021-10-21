import io
from typing import Iterable
from ...data.readers.internals.base_inner_reader import BaseInnerReader, BUFFER_SIZE


class DiskReader(BaseInnerReader):
    def __init__(self, **kwargs):
        """
        File System Reader
        """
        super().__init__(**kwargs)

    def get_blobs_at_path(self, path):
        if path.exists():
            blobs = path.rglob("*")
            return [blob.as_posix() for blob in blobs if blob.is_file()]
        return []

    def get_blob_stream(self, blob_name: str) -> io.IOBase:
        with open(blob_name, "rb") as f:
            io_stream = io.BytesIO(f.read())
            return io_stream
