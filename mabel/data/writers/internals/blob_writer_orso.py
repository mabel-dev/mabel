import datetime
import io
import sys
import threading

import orso
import zstandard
from orso.logging import get_logger

from mabel.data.validator import Schema
from mabel.errors import MissingDependencyError

BLOB_SIZE = 64 * 1024 * 1024  # 64Mb, 16 files per gigabyte
SUPPORTED_FORMATS_ALGORITHMS = ("jsonl", "zstd", "parquet", "text", "flat")


def get_size(obj, seen=None):
    """
    Recursively approximate the size of objects.
    We don't know the actual size until we save, so we approximate the size based
    on some rules - this will be wrong due to RLE, headers, precision and other
    factors.
    """
    obj_type = type(obj)
    obj_id = id(obj)

    if seen is None:
        seen = set()

    if obj_id in seen:
        return 0

    if obj_type == int or obj_type == float:
        size = 6
    elif obj_type == bool:
        size = 1
    elif obj_type in {str, bytes, bytearray}:
        size = len(obj) + 4
    elif obj is None:  # NoneType is not directly accessible
        size = 1
    elif obj_type == datetime.datetime:
        size = 8
    else:
        size = sys.getsizeof(obj)

    # Mark as seen before entering recursion to gracefully handle
    # self-referential objects
    seen.add(obj_id)

    if obj_type == dict:
        size += sum(get_size(v, seen) for v in obj.values()) + 8
    elif hasattr(obj, "__dict__"):
        size += get_size(obj.__dict__, seen) + 8
    elif hasattr(obj, "__iter__") and obj_type not in {str, bytes, bytearray}:
        size += sum(get_size(i, seen) for i in obj) + 8

    return size


class BlobWriter(object):
    wal = []

    def __init__(
        self,
        *,  # force params to be named
        inner_writer=None,  # type:ignore
        blob_size: int = BLOB_SIZE,
        format: str = "zstd",
        schema: Schema = None,
        **kwargs,
    ):
        self.format = format
        self.maximum_blob_size = blob_size

        if format not in SUPPORTED_FORMATS_ALGORITHMS:
            raise ValueError(
                f"Invalid format `{format}`, valid options are {SUPPORTED_FORMATS_ALGORITHMS}"
            )

        kwargs["format"] = format
        self.inner_writer = inner_writer(**kwargs)  # type:ignore

        self.schema = None
        if isinstance(schema, (list, dict)):
            schema = Schema(schema)
        if isinstance(schema, Schema):
            self.schema = schema
        if self.schema is None:
            self.schema = []
            raise MissingDependencyError("Writers require a Schema")
        self.schema = self.schema.schema

        self.wal = orso.DataFrame(rows=[], schema=self.schema)
        self.records_in_buffer = 0
        self.byte_count = 5120

    def append(self, record: dict):
        self.wal.append(record)
        self.records_in_buffer += 1
        self.byte_count += get_size(record)
        # if this write would exceed the blob size, close it
        if self.byte_count >= self.maximum_blob_size:
            self.commit()
            self.wal = orso.DataFrame(rows=[], schema=self.schema)
            self.records_in_buffer = 0
            self.byte_count = 5120

        return self.records_in_buffer

    def commit(self):
        committed_blob_name = ""

        if len(self.wal) > 0:
            lock = threading.Lock()

            try:
                lock.acquire(blocking=True, timeout=10)

                if self.format == "parquet":
                    try:
                        import pyarrow.parquet
                    except ImportError:
                        raise MissingDependencyError(
                            "`pyarrow` missing, please install or include in `requirements.txt`."
                        )
                    pytable = self.wal.arrow()

                    tempfile = io.BytesIO()
                    pyarrow.parquet.write_table(pytable, where=tempfile, compression="zstd")

                    tempfile.seek(0)
                    buffer = tempfile.read()

                else:
                    buffer = b"\n".join(r.as_json for r in self.wal) + b"\n"
                    if self.format == "zstd":
                        # zstandard is an non-optional installed dependency
                        buffer = zstandard.compress(buffer)

                committed_blob_name = self.inner_writer.commit(
                    byte_data=bytes(buffer), override_blob_name=None
                )

                if "BACKOUT" in committed_blob_name:
                    get_logger().warning(
                        f"{self.records_in_buffer:n} failed records written to BACKOUT partition `{committed_blob_name}`"
                    )
                get_logger().debug(
                    {
                        "format": self.format,
                        "committed_blob": committed_blob_name,
                        "records": (
                            len(buffer) if self.format == "parquet" else self.records_in_buffer
                        ),
                    }
                )
            finally:
                lock.release()

        self.wal = orso.DataFrame(rows=[], schema=self.schema)
        self.records_in_buffer = 0
        self.byte_count = 5120
        return committed_blob_name

    def __del__(self):
        # this should never be relied on to save data
        self.commit()
