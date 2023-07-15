import datetime
import sys
import threading

import orso
import zstandard
from mabel.data.validator import Schema
from mabel.errors import MissingDependencyError
from mabel.logging import get_logger

BLOB_SIZE = 64 * 1024 * 1024  # 64Mb, 16 files per gigabyte
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

        self.schema = schema
        self.open_buffer()

    def append(self, record: dict):
        """
        add a new row to the write ahead log
        """
        if self.wal.nbytes > BLOB_SIZE:
            self.commit()
            self.open_buffer()

        self.wal.append(record)
        self.records_in_buffer += 1
        return self.records_in_buffer

    def _normalize_arrow_schema(self, table, mabel_schema):
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
            "VARCHAR": pyarrow.string(),
            "BOOLEAN": pyarrow.bool_(),
            "NUMERIC": pyarrow.float64(),
            "LIST": pyarrow.list_(pyarrow.string())
            #            "STRUCT": pyarrow.map_(pyarrow.string(), pyarrow.string())
        }

        schema = table.schema

        for column in schema.names:
            # if we know about the column and it's a type we handle
            if column in mabel_schema and mabel_schema[column] in type_map:
                index = table.column_names.index(column)
                # update the schema
                schema = schema.set(index, pyarrow.field(column, type_map[mabel_schema[column]]))
        # apply the updated schema
        table = table.cast(target_schema=schema)
        return table

    def commit(self):
        committed_blob_name = ""

        if len(self.wal) > 0:
            lock = threading.Lock()

            try:
                lock.acquire(blocking=True, timeout=10)

                if self.format == "parquet":
                    try:
                        import pyarrow
                        import pyarrow.parquet
                    except ImportError as err:  # pragma: no cover
                        raise MissingDependencyError(
                            "`pyarrow` is missing, please install or include in requirements.txt"
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
                    # Add in any columns from the schema
                    if self.schema:
                        columns += self.schema.columns
                    columns = sorted(dict.fromkeys(columns))

                    # then we make sure each row has all the columns
                    self.buffer = [
                        {column: row.get(column) for column in columns} for row in self.buffer
                    ]

                    pytable = pyarrow.Table.from_pylist(self.buffer)

                    # if we have a schema, make effort to align the parquet file to it
                    if self.schema:
                        pytable = self._normalize_arrow_schema(pytable, self.schema)

                    pyarrow.parquet.write_table(pytable, where=tempfile, compression="zstd")

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
                        "bytes": self.byte_count if self.format == "parquet" else len(self.buffer),
                    }
                )
            finally:
                lock.release()

        self.open_buffer()
        return committed_blob_name

    def open_buffer(self):
        self.wal = orso.DataFrame(row=[], schema=self.schema)
        self.records_in_buffer = 0

    def __del__(self):
        # this should never be relied on to save data
        self.commit()
