import time
import datetime
from typing import Any
from dateutil import parser
from .simple_writer import SimpleWriter
from .internals.blob_writer import BlobWriter
from ..validator import Schema  # type:ignore
from ...utils import paths
from ...errors import ValidationError, InvalidDataSetError
from ...logging import get_logger
from ...data.formats import json


class BatchWriter(SimpleWriter):

    def __init__(
            self,
            *,
            dataset: str,
            format: str = 'zstd',
            date: Any = None,
            frame_id: str = None, 
            **kwargs):
        """
        The batch data writer to writes data records into blobs. Batches
        are written into timestamped folders called Partitions.

        Parameters:
            dataset: string (optional)
                The name of the dataset - this is used to map to a path
            schema: mabel.validator.Schema (optional)
                Schema used to test records for conformity, default is no 
                schema and therefore no validation
            format: string (optional)
                - jsonl: raw json lines
                - lzma: lzma compressed json lines
                - zstd: zstandard compressed json lines (default)
                - parquet: Apache Parquet
            date: date or string (optional)
                A date, a string representation of a date to use for
                creating the dataset. The default is today's date
            blob_size: integer (optional)
                The maximum size of blobs, the default is 64Mb
            inner_writer: BaseWriter (optional)
                The component used to commit data, the default writer is the
                NullWriter
            frame_id: string (optional)
            raw_path: boolean (optional)
                Don't add any date 

        Note:
            Different inner_writers may take or require additional parameters.
        """
        # call this now, we need this value before we call the base class init
        self.batch_date = self._get_writer_date(date)
        # because jobs can be split, allow a frame_id to be passed in
        if not frame_id:
            frame_id = BatchWriter.create_frame_id()

        self.dataset = dataset
        if "{date" not in self.dataset and not kwargs.get('raw_path', False):
            self.dataset += '/{datefolders}'
        self.dataset = paths.build_path(
                self.dataset + '/' + frame_id,   # type:ignore
                self.batch_date)

        kwargs['raw_path'] = True  # we've just added the dates
        kwargs['format'] = format
        kwargs['dataset'] = self.dataset

        super().__init__(**kwargs)

        # create the writer
        self.blob_writer = BlobWriter(**kwargs)

    @staticmethod
    def create_frame_id():
        return datetime.datetime.now().strftime('as_at_%Y%m%d-%H%M%S')
