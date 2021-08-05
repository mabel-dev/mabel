import os
import threading
import tempfile
import collections
from typing import Any
from orjson import dumps
from ....logging import get_logger
from ....utils.paths import get_parts
from ....utils import safe_field_name
from ....index.index import IndexBuilder
from ....errors import MissingDependencyError


BLOB_SIZE = 64 * 1024 * 1024  # 64Mb, 16 files per gigabyte
BUFFER_SIZE = BLOB_SIZE  # buffer in memory really
SUPPORTED_FORMATS_ALGORITHMS = ("jsonl", "lzma", "zstd", "parquet", "text")


def flatten(dictionary, parent_key=False, separator="."):
    """
    Turn a nested dictionary into a flattened dictionary
    :param dictionary: The dictionary to flatten
    :param parent_key: The string to prepend to dictionary's keys
    :param separator: The string used to separate flattened keys
    :return: A flattened dictionary
    """
    items = []
    for key, value in dictionary.items():
        new_key = str(parent_key) + separator + key if parent_key else key
        if isinstance(value, collections.MutableMapping):
            items.extend(flatten(value, new_key, separator).items())
        elif isinstance(value, list):
            for k, v in enumerate(value):
                items.extend(flatten({str(k): v}, new_key).items())
        else:
            items.append((new_key, value))
    return dict(items)


class BlobWriter(object):

    # in som failure scenarios commit is called before __init__, so we need to define
    # this variable outside the __init__.
    bytes_in_blob = 0

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

        self._open_blob()

    def append(self, record: dict = {}):
        # serialize the record
        if self.format == "text":
            serialized = str(record).encode() + b"\n"
        else:
            serialized = dumps(flatten(record)) + b"\n"  # type:ignore

        # add the columns to the index
        for column in self.indexes:
            self.index_builders[column].add(self.records_in_blob, record)

        # the newline isn't counted so add 1 to get the actual length
        # if this write would exceed the blob size, close it so another
        # blob will be created
        self.bytes_in_blob += len(serialized) + 1
        if self.bytes_in_blob > self.maximum_blob_size and self.records_in_blob > 0:
            self.bytes_in_blob -= len(serialized) + 1
            self.commit()
            self._open_blob()

        # write the record to the file
        self.file.write(serialized)
        self.records_in_blob += 1

        return self.records_in_blob

    def commit(self):

        committed_blob_name = ""

        if self.bytes_in_blob > 0:
            with threading.Lock():
                try:
                    self.file.flush()
                    self.file.close()
                except ValueError:
                    pass

                if self.format == "parquet":
                    try:
                        from pyarrow import json as js  # type:ignore
                        import pyarrow.parquet as pq  # type:ignore
                    except ImportError as err:  # pragma: no cover
                        raise MissingDependencyError(
                            "`pyarrow` is missing, please install or includein requirements.txt"
                        )

                    table = js.read_json(self.file_name)
                    pq.write_table(
                        table, self.file_name + ".parquet", compression="ZSTD"
                    )
                    self.file_name += ".parquet"

                with open(self.file_name, "rb") as f:
                    byte_data = f.read()

                committed_blob_name = self.inner_writer.commit(
                    byte_data=byte_data, override_blob_name=None
                )

                for column in self.indexes:
                    index = self.index_builders[column].build()

                    bucket, path, stem, suffix = get_parts(committed_blob_name)
                    index_name = (
                        bucket
                        + "/"
                        + path
                        + "_SYS."
                        + stem
                        + "."
                        + safe_field_name(column)
                        + ".index"
                    )

                    index_stream = index._index
                    index_stream.seek(0)
                    committed_index_name = self.inner_writer.commit(
                        byte_data=index_stream.read(), override_blob_name=index_name
                    )

                if "BACKOUT" in committed_blob_name:
                    get_logger().warning(
                        f"{self.records_in_blob:n} failed records written to BACKOUT partition `{committed_blob_name}`"
                    )
                get_logger().debug(
                    {
                        "committed_blob": committed_blob_name,
                        "records": self.records_in_blob,
                        "raw_bytes": self.bytes_in_blob,
                        "committed_bytes": len(byte_data),
                    }
                )
                try:
                    os.remove(self.file_name)
                except ValueError:
                    pass

                self.bytes_in_blob = 0
                self.file_name = None

        return committed_blob_name

    def _open_blob(self):
        self.file_name = self._create_temp_file_name()
        self.file: Any = open(self.file_name, mode="wb", buffering=BUFFER_SIZE)
        if self.format == "lzma":
            import lzma

            self.file = lzma.open(self.file, mode="wb")
        if self.format == "zstd":
            import zstandard  # type:ignore

            self.file = zstandard.open(self.file_name, mode="wb")

        # create index builders
        self.index_builders = {}
        for column in self.indexes:
            self.index_builders[column] = IndexBuilder(column)

        self.bytes_in_blob = 0
        self.records_in_blob = 0

    def __del__(self):
        # this should never be relied on to save data
        self.commit()

    def _create_temp_file_name(self):
        """
        Create a tempfile, get the name and then deletes the tempfile.

        The behaviour of tempfiles is inconsistent between operating systems,
        this helps to ensure consistent behaviour.
        """
        file = tempfile.NamedTemporaryFile(prefix="mabel-", delete=True)
        file_name = file.name
        file.close()
        try:
            os.remove(file_name)
        except OSError:
            pass
        return file_name
