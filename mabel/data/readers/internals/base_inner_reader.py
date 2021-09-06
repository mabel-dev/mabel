"""
Base Inner Reader
"""
import abc
import pathlib
import datetime
from io import IOBase
from typing import Iterable
from ....utils import paths, dates
from ....logging import get_logger


BUFFER_SIZE: int = 64 * 1024 * 1024  # 64Mb


class BaseInnerReader(abc.ABC):
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

        self.start_date = dates.extract_date(kwargs.get("start_date"))
        self.end_date = dates.extract_date(kwargs.get("end_date"))

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

    def get_list_of_blobs(self):

        blobs = []
        # for each day in the range, get the blobs for us to read
        for cycle_date in dates.date_range(self.start_date, self.end_date):
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
