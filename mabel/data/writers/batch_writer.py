import datetime
import os
from typing import Any

import orjson
from orso.logging import get_logger

from mabel.data.writers.internals.blob_writer import BlobWriter
from mabel.data.writers.writer import Writer
from mabel.utils import paths

logger = get_logger()


class BatchWriter(Writer):
    """
    Extend the functionality of the Writer to better support batch data
    """

    def __init__(
        self,
        *,
        dataset: str,
        format: str = "zstd",
        date: Any = None,
        partitions=["year_{yyyy}/month_{mm}/day_{dd}"],
        frame_id: str = None,
        metadata: dict = None,
        always_complete: bool = False,
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
                - flat: flattened json records in json lines
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
            metadata: dict (optional)
                data to write into the frame.complete file
            always_complete: bool (optional)
                Write a frame.complete even if zero records are written.

        Note:
            Different inner_writers may take or require additional parameters.
        """
        # because jobs can be split, allow a frame_id to be passed in
        if not frame_id:
            # as_ats are in UTC
            as_at = datetime.datetime.utcnow().strftime("as_at_%Y%m%d-%H%M%S")
            self.batch_date = self._get_writer_date(date)

            # we now use partitions
            # if "{date" not in dataset and not kwargs.get("raw_path", False):
            #    dataset += "/{datefolders}"
            if partitions:
                dataset += "/" + "/".join(partitions)
            frame_id = paths.build_path(
                dataset + "/" + as_at,  # type:ignore
                self.batch_date,
            )

        if metadata:
            self.metadata = metadata
        else:
            self.metadata = {}

        # kwargs["raw_path"] = True  # we've just added the dates
        kwargs["format"] = format
        kwargs["dataset"] = frame_id
        kwargs["partitions"] = partitions

        super().__init__(**kwargs)

        self.dataset = frame_id
        kwargs["schema"] = self.schema

        # create the writer
        self.blob_writer = BlobWriter(**kwargs)

        # in finalize is called more than once - it can be called with the
        # error cleared from the stack.
        self.seen_failures = False

        self.always_complete = always_complete

    def finalize(self, **kwargs):
        import orso

        import mabel

        final = super().finalize()

        has_failure = bool(kwargs.get("has_failure", False))

        if has_failure:
            self.seen_failures = True
            get_logger().debug(f"Error found in the stack, not marking frame as complete.")
            return -1

        if self.seen_failures:
            get_logger().debug(
                f"Error previously seen in the stack, not marking frame as complete."
            )
            return -1

        if self.records == 0 and not self.always_complete:
            get_logger().warning(
                f"No records written, and 'always_complete' not set, so not marking frame as complete."
            )
            return -1

        completion_path = self.blob_writer.inner_writer.filename
        completion_path = os.path.split(completion_path)[0] + "/frame.complete"
        self.metadata["records"] = self.records
        self.metadata["format"] = self.blob_writer.format
        if self.schema:
            if isinstance(self.schema, dict):
                self.metadata["schema"] = self.schema
            elif hasattr(self.schema, "to_dict"):
                self.metadata["schema"] = self.schema.to_dict()
        self.metadata["manifest"] = self.blob_writer.manifest
        # write the library versions - this allows us to change logic later if
        # the file format changes
        self.metadata["versions"] = {"mabel": mabel.__version__, "orso": orso.__version__}
        flag = self.blob_writer.inner_writer.commit(
            byte_data=orjson.dumps(
                self.metadata, option=orjson.OPT_INDENT_2 | orjson.OPT_SERIALIZE_NUMPY
            ),
            override_blob_name=completion_path,
        )
        get_logger().debug(f"Frame completion file `{flag}` written")
        return final
