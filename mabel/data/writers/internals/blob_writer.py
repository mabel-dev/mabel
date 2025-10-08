import io
import json
import threading
from typing import List, Optional, Union

import orjson
import orso
import zstandard
from orso.logging import get_logger
from orso.schema import RelationSchema

from mabel.data.internals.records import flatten
from mabel.data.validator import schema_loader
from mabel.errors import MissingDependencyError

# we use 62Mb to allow for headers/footers and errors in calcs
BLOB_SIZE = 62 * 1024 * 1024  # 64Mb, 16 files per gigabyte
SUPPORTED_FORMATS_ALGORITHMS = ("jsonl", "zstd", "parquet", "text", "flat")


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
        format: str = "parquet",
        schema: Optional[RelationSchema] = None,
        parquet_row_group_size: int = 5000,
        sort_by: Optional[Union[str, List]] = None,
        use_dictionary: Optional[Union[bool, List[str]]] = None,
        **kwargs,
    ):
        self.format = format
        self.maximum_blob_size = blob_size
        self.parquet_row_group_size = parquet_row_group_size
        self.sort_by = sort_by
        self.use_dictionary = use_dictionary

        if format not in SUPPORTED_FORMATS_ALGORITHMS:
            raise ValueError(
                f"Invalid format `{format}`, valid options are {SUPPORTED_FORMATS_ALGORITHMS}"
            )

        kwargs["format"] = format
        self.inner_writer = inner_writer(**kwargs)  # type:ignore
        self.schema = schema_loader(schema)
        self.open_buffer()

        if self.format == "parquet":
            self.append = self.arrow_append
        else:
            self.append = self.text_append

    def arrow_append(self, record: dict = {}):
        self.records_in_buffer += 1
        self.wal.append(record)  # type:ignore
        # if this write would exceed the blob size, close it
        if self.wal.nbytes() > self.maximum_blob_size:
            self.commit()
            self.open_buffer()

        return self.records_in_buffer

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
        if len(self.buffer) > self.maximum_blob_size:
            self.commit()
            self.open_buffer()

        if isinstance(self.buffer, bytes):
            self.buffer = bytearray(self.buffer)
            get_logger().warning("Write buffer corrected from invalid state.")
        # write the record to the file
        self.buffer.extend(serialized)
        self.records_in_buffer += 1

        return self.records_in_buffer

    def _normalize_arrow_schema(self, table, mabel_schema: RelationSchema):
        """
        Because we partition the data, there are instances where nulls in one of the
        columns isn't being correctly identified as the target type.

        We only handle a subset of types here, so it doesn't remove the problem.
        """
        try:
            import pyarrow
        except ImportError:
            raise MissingDependencyError(
                "`pyarrow` missing, please install or include in `requirements.txt`."
            )

        type_map = {
            "TIMESTAMP": pyarrow.timestamp("us"),
            "DATE": pyarrow.date64(),
            "VARCHAR": pyarrow.string(),
            "BOOLEAN": pyarrow.bool_(),
            "INTEGER": pyarrow.int64(),
            "DOUBLE": pyarrow.float64(),
            "ARRAY": pyarrow.list_(pyarrow.string()),
            "BLOB": pyarrow.binary(),
            #            "STRUCT": pyarrow.map_(pyarrow.string(), pyarrow.string())
        }

        schema = table.schema

        for column in schema.names:
            # if we know about the column and it's a type we handle
            mabel_column = mabel_schema.find_column(column)
            if mabel_column and mabel_column.type in type_map:
                index = table.column_names.index(column)
                # update the schema
                schema = schema.set(
                    index, pyarrow.field(column, type_map[mabel_column.type])
                )
        # apply the updated schema
        table = table.cast(target_schema=schema)
        return table

    def commit(self):
        committed_blob_name = ""

        if self.records_in_buffer > 0:
            lock = threading.Lock()

            try:
                lock.acquire(blocking=True, timeout=10)

                if self.format == "parquet":
                    try:
                        import pyarrow
                        import pyarrow.parquet
                    except ImportError:  # pragma: no cover
                        raise MissingDependencyError(
                            "`pyarrow` is missing, please install or include in requirements.txt"
                        )

                    pytable = self.wal.arrow()

                    # if we have a schema, make effort to align the parquet file to it
                    if self.schema:
                        pytable = self._normalize_arrow_schema(pytable, self.schema)

                    # sort the table if sort_by is specified
                    if self.sort_by:
                        # Convert list of strings to PyArrow format
                        sort_spec = self.sort_by
                        if isinstance(self.sort_by, list) and all(isinstance(item, str) for item in self.sort_by):
                            # Convert list of strings to list of tuples with default ascending order
                            sort_spec = [(col, "ascending") for col in self.sort_by]
                        pytable = pytable.sort_by(sort_spec)

                    tempfile = io.BytesIO()
                    write_kwargs = {"row_group_size": self.parquet_row_group_size}
                    if self.use_dictionary is not None:
                        write_kwargs["use_dictionary"] = self.use_dictionary
                    pyarrow.parquet.write_table(pytable, where=tempfile, **write_kwargs)

                    tempfile.seek(0)
                    write_buffer = tempfile.read()

                elif self.format == "zstd":
                    # zstandard is an non-optional installed dependency
                    write_buffer = zstandard.compress(self.buffer)
                else:
                    write_buffer = bytes(self.buffer)

                committed_blob_name = self.inner_writer.commit(
                    byte_data=write_buffer, override_blob_name=None
                )

                if "BACKOUT" in committed_blob_name:
                    get_logger().warning(
                        f"{self.records_in_buffer:n} failed records written to BACKOUT partition `{committed_blob_name}`"
                    )

                get_logger().debug(
                    {
                        "format": self.format,
                        "committed_blob": committed_blob_name,
                        "records": self.records_in_buffer,
                        "bytes": len(write_buffer),
                    }
                )
            finally:
                lock.release()

        self.open_buffer()
        return committed_blob_name

    def open_buffer(self):
        if self.format == "parquet":
            self.wal = orso.DataFrame(rows=[], schema=self.schema)
        else:
            self.buffer = bytearray()
            self.byte_count = 0
        self.records_in_buffer = 0

    def __del__(self):
        # this should never be relied on to save data
        self.commit()
