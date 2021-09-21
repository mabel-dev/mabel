import threading
import simdjson
from typing import Any
from orjson import dumps
import zstandard
from ...internals.index import IndexBuilder
from ...internals.records import flatten
from ....logging import get_logger
from ....utils.paths import get_parts
from ....utils import safe_field_name
from ....errors import MissingDependencyError


BLOB_SIZE = 64 * 1024 * 1024  # 64Mb, 16 files per gigabyte
SUPPORTED_FORMATS_ALGORITHMS = ("jsonl", "zstd", "parquet", "text", "flat")


class BlobWriter(object):

    # in som failure scenarios commit is called before __init__, so we need to define
    # this variable outside the __init__.
    buffer = bytearray()

    def __init__(
        self,
        *,  # force params to be named
        inner_writer=None,  # type:ignore
        blob_size: int = BLOB_SIZE,
        format: str = "zstd",
        **kwargs,
    ):

        self.indexes = kwargs.get("index_on", [])

        self.format = format
        self.maximum_blob_size = blob_size

        if format not in SUPPORTED_FORMATS_ALGORITHMS:
            raise ValueError(
                f"Invalid format `{format}`, valid options are {SUPPORTED_FORMATS_ALGORITHMS}"
            )

        kwargs["format"] = format
        self.inner_writer = inner_writer(**kwargs)  # type:ignore

        self.open_buffer()

    def append(self, record: dict = {}):
        # serialize the record
        if self.format == "text":
            serialized = str(record).encode() + b"\n"
        elif self.format == "flat":
            serialized = dumps(flatten(record)) + b"\n"  # type:ignore
        elif isinstance(record, simdjson.Object):
            serialized = record.mini + b"\n"
        else:
            serialized = dumps(record) + b"\n"  # type:ignore

        # add the columns to the index
        for column in self.indexes:
            self.index_builders[column].add(self.records_in_buffer, record)

        # the newline isn't counted so add 1 to get the actual length
        # if this write would exceed the blob size, close it so another
        # blob will be created
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

            with threading.Lock():

                if self.format == "parquet":
                    try:
                        import pyarrow.json
                        import pyarrow.parquet as pq  # type:ignore
                    except ImportError as err:  # pragma: no cover
                        raise MissingDependencyError(
                            "`pyarrow` is missing, please install or includein requirements.txt"
                        )

                    # pyarrow is opinionated to dealing with files - so we use files
                    # to load into and read from pyarrow
                    # first, we load the buffer into a file and then into pyarrow
                    import tempfile

                    buffer_temp_file = tempfile.TemporaryFile()
                    buffer_temp_file.write(self.buffer)
                    buffer_temp_file.seek(0, 0)
                    in_pyarrow_buffer = pyarrow.json.read_json(buffer_temp_file)
                    buffer_temp_file.close()

                    # then we save from pyarrow into another file which we read
                    pq_temp_file = tempfile.TemporaryFile()
                    pq.write_table(in_pyarrow_buffer, pq_temp_file, compression="ZSTD")
                    pq_temp_file.seek(0, 0)
                    self.buffer = pq_temp_file.read()
                    pq_temp_file.close()

                if self.format == "zstd":
                    # zstandard is an non-optional installed dependency
                    self.buffer = zstandard.compress(self.buffer)

                committed_blob_name = self.inner_writer.commit(
                    byte_data=bytes(self.buffer), override_blob_name=None
                )

                for column in self.indexes:
                    index = self.index_builders[column].build()

                    bucket, path, stem, suffix = get_parts(committed_blob_name)
                    index_name = f"{bucket}/{path}{stem}.{safe_field_name(column)}.idx"
                    committed_index_name = self.inner_writer.commit(
                        byte_data=index.bytes(), override_blob_name=index_name
                    )

                if "BACKOUT" in committed_blob_name:
                    get_logger().warning(
                        f"{self.records_in_buffer:n} failed records written to BACKOUT partition `{committed_blob_name}`"
                    )
                get_logger().debug(
                    {
                        "committed_blob": committed_blob_name,
                        "records": self.records_in_buffer,
                        "bytes": len(self.buffer),
                    }
                )

        self.buffer = bytearray()
        return committed_blob_name

    def open_buffer(self):
        self.buffer = bytearray()

        # create index builders
        self.index_builders = {}
        for column in self.indexes:
            self.index_builders[column] = IndexBuilder(column)

        self.records_in_buffer = 0

    def __del__(self):
        # this should never be relied on to save data
        self.commit()
