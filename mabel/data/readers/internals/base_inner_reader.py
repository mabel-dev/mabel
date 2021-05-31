"""
Base Inner Reader
"""
import os
import abc
import pathlib
import datetime
from io import IOBase
from typing import Iterable, Optional
from dateutil import parser
from ...formats import json
from ....utils import common, paths
from ....logging import get_logger
from ....errors import MissingDependencyError


def zstd_reader(stream, rows, all_rows):
    """
    Read zstandard compressed files
    """
    # zstandard should always be present
    import zstandard  # type:ignore

    with zstandard.open(stream, "r", encoding="utf8") as file:  # type:ignore
        for index, row in enumerate(file):
            if all_rows or index in rows:
                yield row


def lzma_reader(stream, rows, all_rows):
    """
    Read LZMA compressed files
    """
    # lzma should always be present
    import lzma

    with lzma.open(stream, "rb") as file:  # type:ignore
        for index, row in enumerate(file):
            if all_rows or index in rows:
                yield row


def parquet_reader(stream, rows, all_rows):
    """
    Read parquet formatted files
    """
    try:
        import pyarrow.parquet as pq  # type:ignore
    except ImportError:  # pragma: no cover
        raise MissingDependencyError(
            "`pyarrow` is missing, please install or include in requirements.txt"
        )
    table = pq.read_table(stream)
    for batch in table.to_batches():
        dict_batch = batch.to_pydict()
        for index in range(len(batch)):
            if all_rows or index in rows:
                yield json.serialize(
                    {k: v[index] for k, v in dict_batch.items()}
                )  # type:ignore


def text_reader(stream, rows, all_rows):
    """
    Default reader, assumes text format
    """
    text = stream.read().decode("utf8")  # type:ignore
    lines = text.splitlines()
    for index, row in enumerate(lines):
        if all_rows or index in rows:
            yield row


READERS = {".zstd": zstd_reader, ".lzma": lzma_reader, ".parquet": parquet_reader}


class BaseInnerReader(abc.ABC):

    VALID_EXTENSIONS = (
        ".txt",
        ".json",
        ".zstd",
        ".lzma",
        ".jsonl",
        ".csv",
        ".lxml",
        ".parquet",
        ".ignore",
        ".profile",
        ".index",
        ".bloom",
        ".complete",
    )

    def _extract_date_part(self, value):
        if isinstance(value, str):
            value = parser.parse(value)
        if isinstance(value, (datetime.date, datetime.datetime)):
            return datetime.date(value.year, value.month, value.day)
        return datetime.date.today()

    def _extract_as_at(self, path):
        parts = path.split("/")
        for part in parts:
            if part.startswith("as_at_"):
                return part
        return ""

    def __init__(self, **kwargs):
        self.dataset = kwargs.get("dataset")
        if self.dataset is None:
            raise ValueError("Readers must have the `dataset` parameter set")
        if not self.dataset.endswith("/"):
            self.dataset += "/"
        if "date" not in self.dataset and not kwargs.get("raw_path", False):
            self.dataset += "{datefolders}/"

        self.start_date = self._extract_date_part(kwargs.get("start_date"))
        self.end_date = self._extract_date_part(kwargs.get("end_date"))

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
    def get_blob_stream(self, blob: str) -> IOBase:
        """
        Return a filelike object
        """
        pass

    def get_records(
        self, blob_name: str, rows: Optional[Iterable[int]] = None
    ) -> Iterable[str]:
        """
        Handle the different file formats.

        Handling here allows the blob stores to be pretty dumb, they
        just need to be able to store and recall blobs.

        Parameters:
            blob_name: string
                The name of the blob to read records from
            rows: list of integers (optional)
                The row numbers of the blob to retrieve records from, the
                default (None) is all of the rows. An empty set returns an
                empty Iterable from this method

        Returns:
            Iterable of the rows
        """
        # if the rows has been set but the set is empty, return empty list
        if isinstance(rows, (set, list)):
            if len(rows) == 0:
                return []
            rows = set(rows)
            all_rows = False
        else:
            all_rows = True
            rows = set([-1])

        path, ext = os.path.splitext(blob_name)
        stream = self.get_blob_stream(blob_name)

        yield from READERS.get(ext, text_reader)(stream, rows, all_rows)

    def get_list_of_blobs(self):

        blobs = []
        for cycle_date in common.date_range(self.start_date, self.end_date):
            # build the path name
            cycle_path = pathlib.Path(
                paths.build_path(path=self.dataset, date=cycle_date)
            )
            blobs += list(self.get_blobs_at_path(path=cycle_path))

        # remove any BACKOUT data
        blobs = [blob for blob in blobs if "BACKOUT" not in blob]

        # work out if there's an as_at part
        as_ats = {self._extract_as_at(blob) for blob in blobs if "as_at_" in blob}
        if as_ats:
            as_ats = sorted(as_ats)
            as_at = as_ats.pop()

            is_complete = lambda blobs: any(
                [blob for blob in blobs if as_at + "/frame.complete" in blob]
            )
            is_invalid = lambda blobs: any(
                [blob for blob in blobs if (as_at + "/frame.ignore" in blob)]
            )

            while not is_complete(blobs) or is_invalid(blobs):
                if not is_complete(blobs):
                    get_logger().warning(
                        f"Frame `{as_at}` is not complete - `frame.complete` file is not present - skipping this frame."
                    )
                if is_invalid(blobs):
                    get_logger().debug(
                        f"Frame `{as_at}` is invalid - `frame.ignore` file is present - skipping this frame."
                    )
                if len(as_ats) > 0:
                    as_at = as_ats.pop()
                else:
                    return []
            get_logger().debug(f"Reading from DataSet frame `{as_at}`")
            blobs = [
                blob
                for blob in blobs
                if (as_at in blob) and ("/frame.complete" not in blob)
            ]

        return sorted(blobs)
