import sys
import orjson
import os.path
import datetime

from siphashc import siphash
from typing import Optional, List


from .internals.parallel_reader import ParallelReader, pass_thru
from .internals.multiprocess_wrapper import processed_reader

from ..internals.expression import Expression
from ..internals.records import select_record_fields
from ..internals.dictset import DictSet, STORAGE_CLASS

from ...logging import get_logger
from ...utils.dates import parse_delta
from ...utils.parameter_validator import validate
from ...errors import InvalidCombinationError, DataNotFoundError


# fmt:off
RULES = [
    {"name": "cursor", "required": False, "warning": None, "incompatible_with": []},
    {"name": "dataset", "required": True, "warning": None, "incompatible_with": []},
    {"name": "end_date", "required": False, "warning": None, "incompatible_with": []},
    {"name": "freshness_limit", "required": False, "warning": None, "incompatible_with": []},
    {"name": "inner_reader", "required": False, "warning": None, "incompatible_with": []},
    {"name": "raw_path", "required": False, "warning": None, "incompatible_with": ["freshness_limit"]},
    {"name": "select", "required": False, "warning": None, "incompatible_with": []},
    {"name": "start_date", "required": False, "warning": None, "incompatible_with": []},
    {"name": "query", "required": False, "warning": "", "incompatible_with": ["filters"]},
    {"name": "persistence", "required": False, "warning": "", "incompatible_with": []},
    {"name": "project", "required": False, "warning": "", "incompatible_with": []}
]
# fmt:on




@validate(RULES)
def Reader(
    *,  # force all paramters to be keyworded
    select: list = ["*"],
    dataset: str = None,
    query: Optional[str] = None,
    inner_reader=None,  # type:ignore
    raw_path: bool = False,
    persistence: STORAGE_CLASS = STORAGE_CLASS.NO_PERSISTANCE,
    **kwargs,
) -> DictSet:
    """
    Reads records from a data store, opinionated toward Google Cloud Storage but a
    filesystem reader is available to assist with local development.

    The Reader will iterate over a set of files and return them to the caller as a
    single stream of records. The files can be read from a single folder or can be
    matched over a set of date/time formatted folder names. This is useful to read
    over a set of logs. The date range is provided as part of the call; this is
    essentially a way to partition the data by date/time.

    The reader can filter records to return a subset, for JSON formatted data the
    records can be converted to dictionaries before filtering. JSON data can also
    be used to select columns, so not all read data is returned.

    The reader does not support aggregations, calculations or grouping of data, it
    is a log reader and returns log entries. The reader can convert a set into
    _Pandas_ dataframe, or the _dictset_ helper library can perform some activities
    on the set in a more memory efficient manner.

    Note:
        Different _inner_readers_ may take or require additional parameters. This
        class has a decorator which helps to ensure it is called correctly.

    Parameters:
        select: list of strings (optional):
            A list of the names of the columns to return from the dataset, the
            default is all columns
        dataset: string:
            The path to the data
        query: string (optional):
            An expression which when evaluated for each row, if False the row will
            be removed from the resulant data set, like the WHERE clause of of a SQL
            statement.
        inner_reader: BaseReader (optional):
            The reader class to perform the data access Operators, the default is
            GoogleCloudStorageReader
        start_date: datetime (optional):
            The starting date of the range to read over, default is today
        end_date: datetime (optional):
            The end date of the range to read over, default is today
        freshness_limit: string (optional):
            a time delta string (e.g. 6h30m = 6hours and 30 minutes) which
            incidates the maximum age of a dataset before it is no longer
            considered fresh. Where the 'time' of a dataset cannot be
            determined, it will be treated as midnight (00:00) for the date.
        persistence: STORAGE_CLASS (optional)
            How to cache the results, the default is NO_PERSISTANCE which will almost
            always return a generator. MEMORY should only be used where the dataset
            isn't huge and DISK is many times slower than MEMORY.
        cursor: dictionary (or string)
            Resume read from a given point (assumes other parameters are the same).
            If a JSON string is provided, it will converted to a dictionary.

    Returns:
        DictSet

    Raises:

    """

    if not isinstance(select, list):  # pragma: no cover
        raise TypeError("Reader 'select' parameter must be a list")

    # lazy loading of dependency
    if inner_reader is None:
        from ...adapters.google import GoogleCloudStorageReader

        inner_reader = GoogleCloudStorageReader
    # instantiate the injected reader class

    reader_class = inner_reader(
        dataset=dataset, raw_path=raw_path, **kwargs
    )  # type:ignore

    select = select.copy()

    arg_dict = kwargs.copy()
    arg_dict["select"] = f"{select}"
    arg_dict["dataset"] = f"{dataset}"
    arg_dict["inner_reader"] = f"{inner_reader.__name__}"  # type:ignore
    arg_dict["query"] = query
    get_logger().debug(orjson.dumps(arg_dict))

    # number of days to walk backwards to find records
    freshness_limit = parse_delta(kwargs.get("freshness_limit", ""))

    if (
        freshness_limit and reader_class.start_date != reader_class.end_date
    ):  # pragma: no cover
        raise InvalidCombinationError(
            "freshness_limit can only be used when the start and end dates are the same"
        )

    if query:
        query = Expression(query)

    return DictSet(
        _LowLevelReader(
            reader_class=reader_class,
            freshness_limit=freshness_limit,
            select=select,
            query=query,
        ),
        storage_class=persistence,
    )


def _is_system_file(filename):
    if "_SYS." in filename:
        base = os.path.basename(filename)
        return base.startswith("_SYS.")
    return False


class _LowLevelReader(object):
    def __init__(
        self,
        reader_class,
        freshness_limit,
        select,
        query,
    ):
        self.reader_class = reader_class
        self.freshness_limit = freshness_limit
        self.select = select
        self.query = query

        self._inner_line_reader = None

    def _create_line_reader(self):
        blob_list = self.reader_class.get_list_of_blobs()

        # handle stepping back if the option is set
        if self.freshness_limit > datetime.timedelta(seconds=1):
            while not bool(blob_list) and self.freshness_limit >= datetime.timedelta(
                days=self.reader_class.days_stepped_back
            ):
                self.reader_class.step_back_a_day()
                blob_list = self.reader_class.get_list_of_blobs()
            if self.freshness_limit < datetime.timedelta(
                days=self.reader_class.days_stepped_back
            ):
                get_logger().alert(
                    f"No data found in last {self.freshness_limit} - aborting ({self.reader_class.dataset})"
                )
                sys.exit(5)
            if self.reader_class.days_stepped_back > 0:
                get_logger().warning(
                    f"Stepped back {self.reader_class.days_stepped_back} days to {self.reader_class.start_date} to find last data, my limit is {self.freshness_limit} ({self.reader_class.dataset})"
                )

        readable_blobs = [b for b in blob_list if not _is_system_file(b)]

        if len(readable_blobs) == 0:
            message = f"Reader found {len(readable_blobs)} sources to read data from in `{self.reader_class.dataset}`."
            get_logger().error(message)
            raise DataNotFoundError(message)
        else:
            get_logger().debug(
                f"Reader found {len(readable_blobs)} sources to read data from in `{self.reader_class.dataset}`."
            )

        get_logger().warning("rewrite the cursor functionality")
        # sort the files names, a bit in a bit array represents each file
        # as we read, we turn the bits on
        # we track the current file, and the progress through that
        # the reported cursor is the bit array, currnet file and progress

        parallel = ParallelReader(
            reader=self.reader_class, filter=self.query or pass_thru
        )

        # it takes some effort to set up the multiprocessing, only do it if
        # we have enough files.

        # multi processing has a bug
        if True: #len(readable_blobs) < (2 * cpu_count()) or cpu_count() == 1:
            get_logger().debug("Serial Reader")
            for f in readable_blobs:
                yield from parallel(f)
        else:
            get_logger().debug("Parallel Reader")
            yield from processed_reader(parallel, readable_blobs)


    def __iter__(self):
        return self

    def __next__(self):
        if self._inner_line_reader is None:
            line_reader = self._create_line_reader()
            self._inner_line_reader = line_reader

        # get the the next line from the reader
        record = self._inner_line_reader.__next__()
        if self.select != ["*"]:
            record = select_record_fields(record, self.select)
        return record
