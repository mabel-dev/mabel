"""
Base Inner Reader
"""
import abc
import datetime
import io
import pathlib

from io import IOBase
from functools import lru_cache
from typing import Iterable

from mabel.utils import paths, dates
from mabel.logging import get_logger


BUFFER_SIZE: int = 64 * 1024 * 1024  # 64Mb


@lru_cache(1)
def memcached_server():
    import os

    # the server must be set in the environment
    memcached_config = os.environ.get("MEMCACHED_SERVER", None)
    if memcached_config is None:
        return None

    # expect either SERVER or SERVER:PORT entries
    memcached_config = memcached_config.split(":")
    if len(memcached_config) == 1:
        # the default memcached port
        memcached_config.append(11211)

    # we need the server and the port
    if len(memcached_config) != 2:
        return None

    try:
        from pymemcache.client import base
    except ImportError:
        return None

    # wait 1 second to try to connect, it's not worthwhile as a cache if it's slow
    return base.Client(
        (
            memcached_config[0],
            memcached_config[1],
        ),
        connect_timeout=1,
        timeout=1,
    )


class BaseInnerReader(abc.ABC):
    def _extract_as_at(self, path):
        parts = path.split("/")
        for part in parts:
            if part.startswith("as_at_"):
                return part
        return ""

    def _extract_by(self, path):
        parts = path.split("/")
        for part in parts:
            if part.startswith("by_"):
                return part

    def __init__(self, partitions=None, partition_filter=None, **kwargs):

        today = datetime.datetime.utcnow().replace(minute=0, second=0, microsecond=0)

        self.dataset = kwargs.get("dataset")
        if self.dataset is None:
            raise ValueError("Readers must have the `dataset` parameter set")
        if not self.dataset.endswith("/"):
            self.dataset += "/"
        # if "{" not in self.dataset and not kwargs.get("raw_path", False):
        #    self.dataset += "{datefolders}/"
        if partitions:
            self.dataset += "/".join(partitions) + "/"
        self.partition_filter = partition_filter

        start_date = dates.parse_iso(kwargs.get("start_date")) or today
        end_date = dates.parse_iso(kwargs.get("end_date")) or today

        self.start_date = min(start_date, end_date)
        self.end_date = max(start_date, end_date)

        self.days_stepped_back = 0

    def step_back_a_day(self):
        """
        Steps back a day so data can be read from a previous day
        """
        self.days_stepped_back += 1
        self.start_date -= datetime.timedelta(days=1)
        self.end_date -= datetime.timedelta(days=1)
        return self.days_stepped_back

    def __del__(self):
        """
        Only here in case a helpful dev expects te base class to have it
        """
        pass

    @abc.abstractmethod
    def get_blobs_at_path(self, prefix=None) -> Iterable:
        pass

    @abc.abstractmethod
    def get_blob_bytes(self, blob: str) -> bytes:
        """
        Return a filelike object
        """
        pass

    def read_blob(self, blob: str) -> IOBase:
        """
        Read-thru cache
        """
        cache_server = memcached_server()
        # if cache isn't configured, read and get out of here
        if not cache_server:
            result = self.get_blob_bytes(blob)
            return io.BytesIO(result)

        # hash the blob name for the look up
        from siphashc import siphash

        blob_hash = str(siphash("RevengeOfTheBlob", blob))

        # try to fetch the cached file
        result = cache_server.get(blob_hash)

        # if the item was a miss, get it from storage and add it to the cache
        if result is None:
            result = self.get_blob_bytes(blob)
            cache_server.set(blob_hash, result)

        return io.BytesIO(result)

    def get_list_of_blobs(self):

        visited = {}
        blobs = []
        # For each day in the range, get the blobs for us to read
        for cycle_date in dates.date_range(self.start_date, self.end_date):
            # Build the path name
            cycle_path = pathlib.Path(
                paths.build_path(path=self.dataset, date=cycle_date)
            )
            if not cycle_path in visited:

                visited[cycle_path] = True

                cycle_blobs = list(self.get_blobs_at_path(path=cycle_path))

                # Remove any BACKOUT data - this is essentially a DEAD LETTER queue
                # so we don't want to include in when reading
                cycle_blobs = [blob for blob in cycle_blobs if "BACKOUT" not in blob]

                # The partitions are stored in folders with the prefix 'by_', as in,
                # partitioned **by** field name
                list_of_partitions = {
                    self._extract_by(blob) for blob in cycle_blobs if "/by_" in blob
                }

                # If we've been provided a partition_filter search hint, try to use this
                # first to prune data
                chosen_partition = ""

                if self.partition_filter:
                    from mabel.utils import text

                    # break the filter into parts, and make sure they're safe and valid
                    (
                        partition_filter_field,
                        partition_filter_op,
                        partition_filter_value,
                    ) = self.partition_filter
                    if partition_filter_op not in ("=", "=="):
                        raise NotImplementedError(
                            "`partition_filter` operation can only be equals (`=`)"
                        )
                    partition_filter_field = text.sanitize(partition_filter_field)
                    partition_filter_value = text.sanitize(partition_filter_value)
                    partition_filter = f"/by_{partition_filter_field}/{partition_filter_field}={partition_filter_value}/"

                    # If we can find the partition in the folder set, then prune to it
                    if any(
                        [
                            f"by_{partition_filter_field}" in by
                            for by in list_of_partitions
                        ]
                    ):
                        # Do the pruning
                        cycle_blobs = [
                            blob for blob in cycle_blobs if partition_filter in blob
                        ]
                        #  We only have one partition now
                        list_of_partitions = [f"by_{partition_filter_field}"]
                        get_logger().debug(
                            f"Applied partition filter by: `{partition_filter}`"
                        )
                    else:
                        get_logger().debug(
                            f"Wasn't able to find partition to filter by: `{partition_filter}`"
                        )

                # If we have multiple 'by_' partitions, pick one (pick the first one)
                if list_of_partitions:
                    list_of_partitions = sorted(list_of_partitions)
                    chosen_partition = list_of_partitions.pop()
                    if list_of_partitions:
                        get_logger().info(
                            f"Ignoring {len(list_of_partitions)} 'by' partitionings, reading from '{chosen_partition}'"
                        )
                    # Do the pruning
                    cycle_blobs = [
                        blob for blob in cycle_blobs if f"/{chosen_partition}/" in blob
                    ]

                def safe_get_next(lst, item):
                    try:
                        index = lst.index(item)
                        return lst[index + 1]
                    except:
                        return None

                # Cycle over the list of partitions (e.g. the hour=02 bits) we can't use
                # the frame id of one on the rest
                if chosen_partition == "":
                    partitioned_folders = {""}
                else:
                    partitioned_folders = {
                        safe_get_next(blob.split("/"), chosen_partition)
                        for blob in cycle_blobs
                    }

                for partitioned_folder in partitioned_folders:

                    partitioned_blobs = [
                        blob
                        for blob in cycle_blobs
                        if f"{chosen_partition}/{partitioned_folder}" in blob
                    ]

                    # Work out if there's an as_at part
                    as_ats = {
                        self._extract_as_at(blob)
                        for blob in partitioned_blobs
                        if "as_at_" in blob
                    }
                    if as_ats:
                        as_ats = sorted(as_ats)
                        as_at = as_ats.pop()

                        is_complete = lambda blobs: any(
                            [
                                blob
                                for blob in blobs
                                if as_at + "/frame.complete" in blob
                            ]
                        )
                        is_invalid = lambda blobs: any(
                            [
                                blob
                                for blob in blobs
                                if (as_at + "/frame.ignore" in blob)
                            ]
                        )

                        while not is_complete(partitioned_blobs) or is_invalid(
                            partitioned_blobs
                        ):
                            if not is_complete(partitioned_blobs):
                                get_logger().debug(
                                    f"Frame `{partitioned_folder}/{as_at}` is not complete - `frame.complete` file is not present - skipping this frame."
                                )
                            if is_invalid(partitioned_blobs):
                                get_logger().debug(
                                    f"Frame `{partitioned_folder}/{as_at}` is invalid - `frame.ignore` file is present - skipping this frame."
                                )
                            if len(as_ats) > 0:
                                as_at = as_ats.pop()
                            else:
                                return []
                        get_logger().debug(f"Reading from DataSet frame `{as_at}`")
                        partitioned_blobs = [
                            blob
                            for blob in partitioned_blobs
                            if (as_at in blob) and ("/frame.complete" not in blob)
                        ]

                    blobs += partitioned_blobs

        return sorted(blobs)
