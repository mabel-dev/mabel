import datetime
import json
import orjson
import sys
import threading
import zstandard

from mabel.data.internals.records import flatten
from mabel.logging import get_logger
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
    size = sys.getsizeof(obj)

    if seen is None:
        seen = set()
    obj_id = id(obj)
    if obj_id in seen:
        return 0

    if isinstance(obj, (int, float)):
        size = 8  # probably 4 bytes
    if isinstance(obj, bool):
        size = 1
    if isinstance(obj, (str, bytes, bytearray)):
        size = len(obj) + 4
    if obj is None:
        size = 1
    if isinstance(obj, datetime.datetime):
        size = 8

    # Important mark as seen *before* entering recursion to gracefully handle
    # self-referential objects
    seen.add(obj_id)
    if isinstance(obj, dict):
        size = sum([get_size(v, seen) for v in obj.values()]) + 8
    elif hasattr(obj, "__dict__"):
        size += get_size(obj.__dict__, seen) + 8
    elif hasattr(obj, "__iter__") and not isinstance(obj, (str, bytes, bytearray)):
        size += sum([get_size(i, seen) for i in obj]) + 8
    return size


class BlobWriter(object):

    # in som failure scenarios commit is called before __init__, so we need to define
    # this variable outside the __init__.
    buffer = bytearray()
    byte_count = 0

    def __init__(
        self,
        *,  # force params to be named
        inner_writer=None,  # type:ignore
        blob_size: int = BLOB_SIZE,
        format: str = "zstd",
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

        self.open_buffer()

        if self.format == "parquet":
            self.append = self.arrow_append
        else:
            self.append = self.text_append

    def arrow_append(self, record: dict = {}):
        record_length = get_size(record)
        # if this write would exceed the blob size, close it
        if (
            self.byte_count + record_length
        ) > self.maximum_blob_size and self.records_in_buffer > 0:
            self.commit()
            self.open_buffer()

        self.byte_count += record_length + 8
        self.records_in_buffer += 1
        self.buffer.append(record)  # type:ignore

    def text_append(self, record: dict = {}):
        # serialize the record
        if self.format == "text":
            if isinstance(record, bytes):
                serialized = record + b"\n"
            elif isinstance(record, str):
                serialized = record.encode() + b"\n"
            else:
                serialized = str(record).encode() + b"\n"
        elif self.format == "flat":
            serialized = orjson.dumps(flatten(record)) + b"\n"  # type:ignore
        elif hasattr(record, "mini"):
            serialized = record.mini + b"\n"  # type:ignore
        else:
            try:
                serialized = orjson.dumps(record) + b"\n"  # type:ignore
            except TypeError:
                serialized = json.dumps(record).encode() + b"\n"

        # the newline isn't counted so add 1 to get the actual length if this write
        # would exceed the blob size, close it so another blob will be created
        if len(self.buffer) > self.maximum_blob_size and self.records_in_buffer > 0:
            self.commit()
            self.open_buffer()

        # write the record to the file
        self.buffer.extend(serialized)
        self.records_in_buffer += 1

        return self.records_in_buffer

    def commit(self):

        committed_blob_name = ""

        if len(self.buffer) > 0:

            lock = threading.Lock()

            try:
                lock.acquire(blocking=True, timeout=10)

                if self.format == "parquet":
                    try:
                        import pyarrow.json
                        import pyarrow.parquet as pq  # type:ignore
                    except ImportError as err:  # pragma: no cover
                        raise MissingDependencyError(
                            "`pyarrow` is missing, please install or includein requirements.txt"
                        )

                    import io
                    from functools import reduce

                    tempfile = io.BytesIO()

                    # When writing to Parquet, the table gets the schema from the first
                    # row, if this row is missing columns (shouldn't, but it happens)
                    # it will be missing for all records, so get the columns from the
                    # entire dataset and ensure all records have the same columns.

                    # first, we get all the columns, from all the records
                    columns = reduce(
                        lambda x, y: x + [a for a in y.keys() if a not in x],
                        self.buffer,
                        [],
                    )

                    # then we make sure each row has all the columns
                    self.buffer = [
                        {column: row.get(column) for column in columns}
                        for row in self.buffer
                    ]

                    pytable = pyarrow.Table.from_pylist(self.buffer)
                    pyarrow.parquet.write_table(
                        pytable, where=tempfile, compression="zstd"
                    )

                    tempfile.seek(0)
                    self.buffer = tempfile.read()

                if self.format == "zstd":
                    # zstandard is an non-optional installed dependency
                    self.buffer = zstandard.compress(self.buffer)

                committed_blob_name = self.inner_writer.commit(
                    byte_data=bytes(self.buffer), override_blob_name=None
                )

                if "BACKOUT" in committed_blob_name:
                    get_logger().warning(
                        f"{self.records_in_buffer:n} failed records written to BACKOUT partition `{committed_blob_name}`"
                    )
                get_logger().debug(
                    {
                        "format": self.format,
                        "committed_blob": committed_blob_name,
                        "records": len(self.buffer)
                        if self.format == "parquet"
                        else self.records_in_buffer,
                        "bytes": self.byte_count
                        if self.format == "parquet"
                        else len(self.buffer),
                    }
                )
            finally:
                lock.release()

        self.open_buffer()
        return committed_blob_name

    def open_buffer(self):
        if self.format == "parquet":
            self.buffer = []
            self.byte_count = 10000  # parquet has headers etc
        else:
            self.buffer = bytearray()
            self.byte_count = 0
        self.records_in_buffer = 0

    def __del__(self):
        # this should never be relied on to save data
        self.commit()
