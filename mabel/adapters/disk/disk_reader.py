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

    def get_blob_bytes(self, blob_name: str) -> bytes:
        with open(blob_name, "rb") as f:
            return f.read()
