import io
from ...data.readers.internals.base_inner_reader import BaseInnerReader


class DiskReader(BaseInnerReader):
    def __init__(self, **kwargs):
        """
        File System Reader
        """
        super().__init__(**kwargs)

    def get_blobs_at_path(self, path):
        if path.exists():
            blobs = path.rglob("*")
            return [
                blob.as_posix()
                for blob in blobs
                if blob.is_file()
                and (
                    blob.suffix in self.VALID_EXTENSIONS
                    or blob.stem in self.VALID_EXTENSIONS
                )
            ]
        return []

    def get_blob_stream(self, blob_name: str) -> io.IOBase:
        with open(blob_name, "rb") as f:
            io_stream = io.BytesIO(f.read())
            return io_stream

    def get_blob_chunk(self, blob_name: str, start: int, buffer_size: int) -> bytes:
        """
        MMAP is by far the fastest way to read files in Python.
        """
        import mmap
        with open(blob_name, mode="rb") as file_obj:
            with mmap.mmap(
                file_obj.fileno(), length=0, access=mmap.ACCESS_READ
            ) as mmap_obj:
                mmap_obj.seek(start, 0)
                return mmap_obj.read(buffer_size)
