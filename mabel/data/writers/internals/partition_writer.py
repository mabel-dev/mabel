import threading
import tempfile
import os
import io
from typing import Any
from ...formats.json import serialize
from ...index import BTree
from ....logging import get_logger

PARTITION_SIZE = 32*1024*1024  # about 32 files per gigabyte
BUFFER_SIZE = PARTITION_SIZE   # buffer in memory really
SUPPORTED_FORMATS_ALGORITHMS = {'jsonl', 'lzma', 'zstd', 'parquet'}

class PartitionWriter():

    def __init__(
            self,
            *,    # force params to be named
            inner_writer = None,  # type:ignore
            partition_size: int = PARTITION_SIZE,
            format: str = 'zstd',
            **kwargs):

        self.format = format
        self.maximum_partition_size = partition_size

        if format not in SUPPORTED_FORMATS_ALGORITHMS:
            raise ValueError(F'Invalid format `{format}`, valid options are {SUPPORTED_FORMATS_ALGORITHMS}')

        kwargs['format'] = format
        self.inner_writer = inner_writer(**kwargs)  # type:ignore
        self.index_on = kwargs.get('index_on', set())
        self._open_partition()


    def append(self, record: dict = {}):
        # serialize the record
        serialized = serialize(record, as_bytes=True) + b'\n'  # type:ignore

        # the newline isn't counted so add 1 to get the actual length
        # if this write would exceed the partition, close it so another
        # partition will be created
        self.bytes_in_partition += len(serialized) + 1
        if self.bytes_in_partition > self.maximum_partition_size:
            self.commit()
            self._open_partition()

        # write the record to the file
        self.file.write(serialized)
        self.records_in_partition += 1

        for field in self.index_on:
            self.indices[field].insert(record.get(field), self.records_in_partition)

        return self.records_in_partition

    def commit(self):

        for field in self.index_on:
            print(self.indices[field].show())
        #get_logger().warning("TODO: indices aren't being saved")

        if self.bytes_in_partition > 0:
            with threading.Lock():
                try:
                    self.file.flush()
                    self.file.close()
                except ValueError:
                    pass

                if self.format == "parquet":
                    from pyarrow import json as js  # type:ignore
                    import pyarrow.parquet as pq    # type:ignore
                    
                    table = js.read_json(self.file_name)
                    pq.write_table(table, self.file_name + '.parquet', compression='ZSTD')
                    self.file_name += '.parquet'

                with open(self.file_name, 'rb') as f:
                    byte_data = f.read()
                    
                committed_partition_name = self.inner_writer.commit(
                        byte_data=byte_data,
                        file_name=None)
                get_logger().debug(F"Partition Committed - {committed_partition_name} - {self.records_in_partition} records, {self.bytes_in_partition} bytes")
                try:
                    os.remove(self.file_name)
                except ValueError:
                    pass

                self.bytes_in_partition = 0
                self.file_name = None

    def _open_partition(self):
        self.file_name = self._create_temp_file_name()
        self.file: Any = open(self.file_name, mode='wb', buffering=BUFFER_SIZE)
        if self.format == 'lzma':
            import lzma
            self.file = lzma.open(self.file, mode='wb')
        if self.format == 'zstd':
            import zstandard  # type:ignore
            self.file = zstandard.open(self.file_name, mode='wb')

        self.bytes_in_partition = 0
        self.records_in_partition = 0
        self.indices = {}
        for field in self.index_on:
            self.indices[field] = BTree()

    def __del__(self):
        # this should never be relied on to save data
        try:
            self.commit()
        except:  # nosec
            pass 

    def _create_temp_file_name(self):
        """
        Create a tempfile, get the name and then deletes the tempfile.

        The behaviour of tempfiles is inconsistent between operating systems,
        this helps to ensure consistent behaviour.
        """
        file = tempfile.NamedTemporaryFile(prefix='mabel-', delete=True)
        file_name = file.name
        file.close()
        try:
            os.remove(file_name)
        except OSError:
            pass
        return file_name
