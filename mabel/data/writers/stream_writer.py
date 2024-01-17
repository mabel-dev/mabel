import datetime
import itertools
import re
import threading
import time

from orso.logging import get_logger

from mabel.data.writers.internals.writer_pool import WriterPool
from mabel.data.writers.writer import Writer
from mabel.utils import dates
from mabel.utils import paths
from mabel.utils import text

# shared lock to we're not adding and removing streams at the same time
lock = threading.Lock()


class StreamWriter(Writer):
    """
    Extend the functionality of the Writer to better support streaming data
    """

    def __init__(
        self,
        *,
        dataset: str,
        format: str = "zstd",
        idle_timeout_seconds: int = 30,
        writer_pool_capacity: int = 10,
        **kwargs,
    ):
        """
        Create a Data Writer to write data records into partitions.

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
            idle_timeout_seconds: integer (optional)
                The number of seconds to wait before evicting writers from the
                pool for inactivity, default is 30 seconds
            writer_pool_capacity: integer (optional)
                The number of writers to leave in the writers pool before
                writers are evicted for over capacity, default is 5
            blob_size: integer (optional)
                The maximum size of blobs, the default is 32Mb
            inner_writer: BaseWriter (optional)
                The component used to commit data, the default writer is the
                NullWriter

        Note:
            Different inner_writers may take or require additional parameters.
        """
        # add the values to kwargs
        kwargs["format"] = format
        kwargs["dataset"] = dataset
        self.dataset = dataset

        super().__init__(**kwargs)

        self.date = dates.parse_iso(kwargs.get("date"))

        self.idle_timeout_seconds = idle_timeout_seconds

        # we have a pool of writers of size maximum_writers
        self.writer_pool_capacity = writer_pool_capacity
        self.writer_pool = WriterPool(pool_size=writer_pool_capacity, **kwargs)

        # establish the background thread responsible for the pool
        self.thread = threading.Thread(target=self.pool_attendant)
        self.thread.name = "mabel-writer-pool-attendant"
        self.thread.daemon = True
        self.run_pool_attendant = True
        self.thread.start()
        get_logger().debug("Pool attendant on-duty")

    def append(self, record: dict):
        """
        Append a new record to the Writer

        Parameters:
            record: dictionary
                The record to append to the Writer

        Returns:
            integer
                The number of records in the current blob
        """
        # get the appropritate writer from the pool and append the record
        # the writer identity is the base of the path where the partitions
        # are written.

        # Check the new record conforms to the schema
        # unlike the batch writer, we don't want to bail out if we have a
        # problem here, instead we're going to save the file to a BACKOUT
        # partition

        writes = 0
        identity = paths.date_format(
            self.dataset_template, (self.date or datetime.datetime.utcnow())
        )

        if hasattr(record, "dict"):
            record = record.dict()
        if hasattr(record, "dump_model"):
            record = record.dump_model()

        elif self.schema:
            try:
                self.schema.validate(record)
            except Exception as e:
                identity += "/BACKOUT/"
                get_logger().warning(
                    f"Schema Validation Failed ({e}) - message being written to {identity}"
                )

        with lock:
            # get the placeholders from the dataset name
            placeholders = set(re.findall(r"\{(.*?)\}", identity))

            # there's no substitutions needed, so just write the record
            if len(placeholders) == 0:
                blob_writer = self.writer_pool.get_writer(identity)
                return blob_writer.append(record)

            # get the values from the record, there can be multiple of these
            values = []
            for placeholder in placeholders:
                value = record.get(placeholder)
                if not isinstance(value, list):
                    value = [value]
                values.append(value)
            # get the cartesian product of these lists
            # save the result to a set otherwise it's not a cartesian product
            value_combinations = {i for i in itertools.product(*values)}

            # for every variation in the cartesian product
            for values in value_combinations:  # type:ignore
                this_identity = identity
                # do the actual replacing of the placeholders
                for k, v in zip(placeholders, values):
                    this_identity = this_identity.replace("{" + k + "}", text.sanitize(str(v)))

                # get the writer and save the record
                blob_writer = self.writer_pool.get_writer(this_identity)
                blob_writer.append(record)
                writes += 1

        return writes

    def finalize(self, **kwargs):
        self.run_pool_attendant = False
        with lock:
            for blob_writer_identity in self.writer_pool.writers:
                try:
                    get_logger().debug(
                        f"Removing from the writer pool during finalization, identity={blob_writer_identity['identity']}, poolsize={len(self.writer_pool.writers)}"
                    )
                    self.writer_pool.remove_writer(blob_writer_identity["identity"])
                except Exception as err:
                    get_logger().debug(
                        f"Error finalizing `{blob_writer_identity}`, {type(err).__name__} - {err}"
                    )
        return super().finalize()

    def pool_attendant(self):
        """
        Writer Pool Management
        """
        while self.run_pool_attendant:
            with lock:
                # search for pool occupants who haven't had a write recently
                for blob_writer_identity in self.writer_pool.get_stale_writers(
                    self.idle_timeout_seconds
                ):
                    get_logger().debug(
                        f"Evicting {blob_writer_identity} from the writer pool due to inactivity - limit is {self.idle_timeout_seconds} seconds, poolsize={len(self.writer_pool.writers)}"
                    )
                    self.writer_pool.remove_writer(blob_writer_identity)
                # if we're over capacity, evict the LRU writers
                for blob_writer_identity in self.writer_pool.nominate_writers_to_evict():
                    get_logger().debug(
                        f"Evicting {blob_writer_identity} from the writer pool due the pool being over its {self.writer_pool_capacity} capacity, poolsize={len(self.writer_pool.writers)}"
                    )
                    self.writer_pool.remove_writer(blob_writer_identity)

            time.sleep(0.1)

        get_logger().debug("Pool attendant off-duty")
