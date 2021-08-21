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
import orjson
from ....utils import common, paths
from ....logging import get_logger
from ....errors import MissingDependencyError


BUFFER_SIZE: int = 64 * 1024 * 1024  # 64Mb


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


def zip_reader(stream, rows, all_rows):
    """
    Read ZIP compressed files
    """
    # zstandard should always be present
    import zipfile

    with zipfile.ZipFile(stream, "r") as zip:
        file = zip.read(zipfile.ZipFile.namelist(zip)[0])
        for index, row in enumerate(file.split(b"\n")):
            if row and (all_rows or index in rows):
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
                yield orjson.dumps(
                    {k: v[index] for k, v in dict_batch.items()}
                ).decode()  # type:ignore


def text_reader(stream, rows, all_rows):
    """
    Default reader, assumes text format
    """
    text = stream.read().decode("utf8")  # type:ignore
    lines = text.splitlines()
    for index, row in enumerate(lines):
        if all_rows or index in rows:
            yield row


READERS = {
    ".zstd": zstd_reader,
    ".lzma": lzma_reader,
    ".parquet": parquet_reader,
    ".zip": zip_reader,
}


class BaseInnerReader(abc.ABC):

    VALID_EXTENSIONS = (
        ".txt",
        ".json",
        ".zstd",
        ".lzma",
        ".zip",
        ".jsonl",
        ".csv",
        ".xml",
        ".lxml",
        ".parquet",
        ".ignore",
        ".index",
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
        if "{" not in self.dataset and not kwargs.get("raw_path", False):
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

    def get_blob_lines(self, blob_name: str) -> Iterable:
        """
        For larger files not written by mabel but we want to read, we need to
        stream the content in rather than load it all at once.
        """
        offset = 0
        carry_forward = b""
        chunk = "INITIALIZED"
        while len(chunk) > 0:
            # we read slightly more than the buffer size to reduce reads for
            # slightly oversized files - it's hard to count to 64M.
            chunk = self.get_blob_chunk(blob_name, offset, BUFFER_SIZE + (1024 * 1024))
            offset += len(chunk)
            lines = (carry_forward + chunk).split(b"\n")
            carry_forward = lines.pop()
            yield from lines
        if carry_forward:
            yield carry_forward

    @abc.abstractmethod
    def get_blob_chunk(self, blob_name: str, start: int, buffer_size: int) -> bytes:
        """
        Read a chunk of the text file
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

        if ext in READERS:
            get_logger().debug(
                f"Reading {'all' if all_rows else len(rows)} rows from `{blob_name}` using a binary reader."
            )
            stream = self.get_blob_stream(blob_name=blob_name)
            yield from READERS.get(ext, text_reader)(
                stream=stream, rows=rows, all_rows=all_rows
            )
        else:
            get_logger().debug(
                f"Reading {'all' if all_rows else len(rows)} rows from `{blob_name}` using the text reader."
            )
            for index, row in enumerate(self.get_blob_lines(blob_name=blob_name)):
                if row and (all_rows or index in rows):
                    yield row.decode("UTF8")

    def get_list_of_blobs(self):

        blobs = []
        for cycle_date in common.date_range(self.start_date, self.end_date):
            # build the path name
            cycle_path = pathlib.Path(
                paths.build_path(path=self.dataset, date=cycle_date)
            )
            cycle_blobs = list(self.get_blobs_at_path(path=cycle_path))

            # remove any BACKOUT data
            cycle_blobs = [blob for blob in cycle_blobs if "BACKOUT" not in blob]

            # work out if there's an as_at part
            as_ats = {
                self._extract_as_at(blob) for blob in cycle_blobs if "as_at_" in blob
            }
            if as_ats:
                as_ats = sorted(as_ats)
                as_at = as_ats.pop()

                is_complete = lambda blobs: any(
                    [blob for blob in blobs if as_at + "/frame.complete" in blob]
                )
                is_invalid = lambda blobs: any(
                    [blob for blob in blobs if (as_at + "/frame.ignore" in blob)]
                )

                while not is_complete(cycle_blobs) or is_invalid(cycle_blobs):
                    if not is_complete(cycle_blobs):
                        get_logger().debug(
                            f"Frame `{as_at}` is not complete - `frame.complete` file is not present - skipping this frame."
                        )
                    if is_invalid(cycle_blobs):
                        get_logger().debug(
                            f"Frame `{as_at}` is invalid - `frame.ignore` file is present - skipping this frame."
                        )
                    if len(as_ats) > 0:
                        as_at = as_ats.pop()
                    else:
                        return []
                get_logger().debug(f"Reading from DataSet frame `{as_at}`")
                cycle_blobs = [
                    blob
                    for blob in cycle_blobs
                    if (as_at in blob) and ("/frame.complete" not in blob)
                ]

            blobs += cycle_blobs

        return sorted(blobs)
