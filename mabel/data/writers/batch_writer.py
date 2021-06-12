import os
import datetime
import traceback
from typing import Any
from .simple_writer import SimpleWriter
from .internals.blob_writer import BlobWriter
from ...utils import paths
from ...data.formats import json
from ...logging import get_logger


class BatchWriter(SimpleWriter):
    def __init__(
        self,
        *,
        dataset: str,
        format: str = "zstd",
        date: Any = None,
        frame_id: str = None,
        **kwargs,
    ):
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
                - text: raw text lines
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
                Don't automatically add any date parts to dataset names
            index_on: collection (optional)
                Index on these columns, the default is to not index

        Note:
            Different inner_writers may take or require additional parameters.
        """
        # call this now, we need this value before we call the base class init
        self.batch_date = self._get_writer_date(date)
        # because jobs can be split, allow a frame_id to be passed in
        if not frame_id:
            frame_id = BatchWriter.create_frame_id()

        self.dataset = dataset
        if "{date" not in self.dataset and not kwargs.get("raw_path", False):
            self.dataset += "/{datefolders}"
        self.dataset = paths.build_path(
            self.dataset + "/" + frame_id,  # type:ignore
            self.batch_date,
        )

        kwargs["raw_path"] = True  # we've just added the dates
        kwargs["format"] = format
        kwargs["dataset"] = self.dataset

        super().__init__(**kwargs)

        self.dataset = paths.build_path(self.dataset, self.batch_date)

        # create the writer
        self.blob_writer = BlobWriter(**kwargs)

    def finalize(self):
        final = super().finalize()

        ex = traceback.format_exc()
        if ex != "NoneType: None\n":
            get_logger().debug(
                f"Error found in the stack, not marking frame as complete."
            )
            return -1

        completion_path = self.blob_writer.inner_writer.filename
        completion_path = os.path.split(completion_path)[0] + "/frame.complete"
        status = {"records": self.records}
        flag = self.blob_writer.inner_writer.commit(
            byte_data=json.serialize(status, as_bytes=True),
            override_blob_name=completion_path,
        )
        get_logger().debug(f"Frame completion file `{flag}` written")
        return final

    @staticmethod
    def create_frame_id():
        return datetime.datetime.now().strftime("as_at_%Y%m%d-%H%M%S")
