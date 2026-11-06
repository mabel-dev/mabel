import copy
import json
import threading
from typing import List
from typing import Optional
from typing import Union

import orjson
import orso
import zstandard
from orso.logging import get_logger
from orso.schema import RelationSchema
from orso.types import OrsoTypes

from mabel.data.internals.records import flatten
from mabel.data.validator import schema_loader
from mabel.errors import MissingDependencyError

# we use 62Mb to allow for headers/footers and errors in calcs
BLOB_SIZE = 62 * 1024 * 1024  # 64Mb, 16 files per gigabyte
SUPPORTED_FORMATS_ALGORITHMS = ("jsonl", "zstd", "parquet", "text", "flat")
# the draken type to write an all-null column as, by its declared mabel type
PARQUET_NULL_TYPES = {
    "TIMESTAMP": "TIMESTAMP",
    "DATE": "DATE",
    "VARCHAR": "VARCHAR",
    "BOOLEAN": "BOOLEAN",
    "INTEGER": "INTEGER",
    "DOUBLE": "DOUBLE",
    "ARRAY": "ARRAY",
    "BLOB": "BLOB",
}


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
        # orso applies the schema when it converts the WAL to Arrow, and a column with
        # no declared type is rendered as a VARCHAR - which fails as soon as anything
        # that isn't a string is written to it. Where we don't know the types, hand
        # orso the column names instead and let it infer them from the data.
        self.wal_schema = self.schema
        if isinstance(self.schema, RelationSchema) and any(
            column.type == OrsoTypes._MISSING_TYPE for column in self.schema.columns
        ):
            self.wal_schema = [column.name for column in self.schema.columns]
        elif isinstance(self.schema, RelationSchema) and any(
            column.type == OrsoTypes.STRUCT for column in self.schema.columns
        ):
            # STRUCT values are held as JSON strings (see `arrow_append`)
            self.wal_schema = copy.deepcopy(self.schema)
            for column in self.wal_schema.columns:
                if column.type == OrsoTypes.STRUCT:
                    column.type = OrsoTypes.VARCHAR
        self.open_buffer()

        if self.format == "parquet":
            self.append = self.arrow_append
        else:
            self.append = self.text_append

    def arrow_append(self, record: dict = {}):
        # parquet is written by rugo, which has no struct type - write nested
        # dictionaries as JSON strings instead
        if any(isinstance(value, dict) for value in record.values()):
            record = {
                key: orjson.dumps(value).decode() if isinstance(value, dict) else value
                for key, value in record.items()
            }
        self.records_in_buffer += 1
        self.wal.append(record)  # type:ignore
        # if this write would exceed the blob size, close it
        # check every 1000 records to reduce overhead.
        if (
            self.records_in_buffer > 0
            and self.records_in_buffer % 1000 == 0
            and self.wal.nbytes() > self.maximum_blob_size
        ):
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

    def _normalize_parquet_schema(self, morsel, mabel_schema: RelationSchema):
        """
        Because we partition the data, a column can be entirely null in one blob, in
        which case its type can't be inferred from the values. Where the schema tells
        us what the column should be, write the nulls as that type so every blob in
        the dataset agrees.
        """
        import draken

        native = draken.draken_native
        null_type = native.DrakenType.NULL
        rows = morsel.num_rows

        names = []
        vectors = []
        changed = False
        for name in morsel.column_names:
            column_name = name.decode() if isinstance(name, bytes) else name
            vector = getattr(morsel.column(name), "_nb", morsel.column(name))
            mabel_column = mabel_schema.find_column(column_name)
            column_type = str(mabel_column.type) if mabel_column else None
            if vector.type == null_type and column_type in PARQUET_NULL_TYPES:
                if column_type == "ARRAY":
                    vector = native.vector_array_from_sequence(
                        [None] * rows, native.DrakenType.VARCHAR, 1
                    )
                else:
                    vector = draken.vector_from_sequence(
                        [None] * rows, PARQUET_NULL_TYPES[column_type]
                    )
                changed = True
            names.append(column_name)
            vectors.append(vector)

        if not changed:
            return morsel
        return draken.Morsel.from_vectors(names, vectors)

    def _sort_parquet(self, morsel):
        """
        Order the rows by `sort_by`, which is either a column name, a list of column
        names, or a list of (column, "ascending" | "descending") tuples. Nulls sort
        last in either direction.

        Returns the sorted morsel, and a (column, descending) tuple for the leading
        key if the file can declare itself sorted by it, otherwise None.
        """
        sort_spec = self.sort_by
        if isinstance(sort_spec, str):
            sort_spec = [sort_spec]
        keys = [
            (item, "ascending") if isinstance(item, str) else (item[0], item[1])
            for item in sort_spec
        ]

        order = list(range(morsel.num_rows))
        # stable sorts, applied from the least to the most significant key
        for column, direction in reversed(keys):
            values = morsel.column(column.encode()).to_pylist()
            present = [i for i in order if values[i] is not None]
            missing = [i for i in order if values[i] is None]
            present.sort(key=values.__getitem__, reverse=direction == "descending")
            order = present + missing

        lead_column, lead_direction = keys[0]
        declared = None
        # rugo records the leading key in the file's sorting_columns, which implies
        # nulls first for ascending and nulls last for descending. We always put nulls
        # last, so only make the claim when that ordering is the same either way.
        lead_nulls = morsel.column(lead_column.encode()).null_count()
        if lead_direction == "descending" or lead_nulls == 0:
            declared = (lead_column, lead_direction == "descending")

        return morsel.take(order), declared

    def commit(self):
        committed_blob_name = ""

        if self.records_in_buffer > 0:
            lock = threading.Lock()

            try:
                lock.acquire(blocking=True, timeout=10)

                if self.format == "parquet":
                    try:
                        from rugo.parquet import write_parquet
                    except ImportError:  # pragma: no cover
                        raise MissingDependencyError(
                            "`rugo` is missing, please install or include in requirements.txt"
                        )

                    morsel = self.wal.to_draken()

                    # if we have a schema, make effort to align the parquet file to it
                    if self.schema:
                        morsel = self._normalize_parquet_schema(morsel, self.schema)

                    write_kwargs = {"max_rows_per_row_group": self.parquet_row_group_size}

                    # sort the table if sort_by is specified
                    if self.sort_by:
                        morsel, declared = self._sort_parquet(morsel)
                        if declared:
                            write_kwargs["sorted_by"] = declared[0]
                            write_kwargs["sorted_descending"] = declared[1]

                    if self.use_dictionary is not None:
                        # rugo decides per column whether dictionary encoding is
                        # worthwhile; it can be turned on or off, but not chosen
                        # column by column, so a list of columns turns it on.
                        write_kwargs["dictionary"] = bool(self.use_dictionary)

                    write_buffer = write_parquet(morsel, **write_kwargs)

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
            self.wal = orso.DataFrame(rows=[], schema=self.wal_schema)
        else:
            self.buffer = bytearray()
            self.byte_count = 0
        self.records_in_buffer = 0

    def __del__(self):
        # this should never be relied on to save data
        self.commit()
