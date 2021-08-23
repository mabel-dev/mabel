import io
import sys
import shutil
import atexit
import orjson
import os.path
import datetime
import cityhash

from typing import Optional, Tuple, List

from .internals.threaded_reader import threaded_reader
from .internals.processed_reader import processed_reader
from .internals.parsers import pass_thru_parser, block_parser, json_parser, xml_parser

from ..internals.dictset import DictSet, STORAGE_CLASS
from ..internals.index import Index
from ..internals.records import select_record_fields
from ..internals.dnf_filters import DnfFilters, get_indexable_filter_columns
from ..internals.expression import Expression


from ...errors import InvalidCombinationError, DataNotFoundError
from ...utils import safe_field_name
from ...utils.parameter_validator import validate
from ...utils.paths import get_parts
from ...utils.dates import parse_delta
from ...logging import get_logger


# available parsers
PARSERS = {
    "json": json_parser,
    "text": pass_thru_parser,
    "block": block_parser,
    "pass-thru": pass_thru_parser,
    "xml": xml_parser,
}

# fmt:off
RULES = [
    {"name": "cache_indexes", "required":False, "warning":None, "incompatible_with": []},
    {"name": "cursor", "required": False, "warning": None, "incompatible_with": ["thread_count", "fork_processes"]},
    {"name": "dataset", "required": True, "warning": None, "incompatible_with": []},
    {"name": "end_date", "required": False, "warning": None, "incompatible_with": []},
    {"name": "extension", "required": False, "warning": None, "incompatible_with": []},
    {"name": "filters", "required": False, "warning": None, "incompatible_with": []},
    {"name": "fork_processes", "required": False, "warning": "Forked Reader may drop records if not used correctly","incompatible_with": ["thread_count"]},
    {"name": "freshness_limit", "required": False, "warning": None, "incompatible_with": []},
    {"name": "inner_reader", "required": False, "warning": None, "incompatible_with": []},
    {"name": "project", "required": False, "warning": None, "incompatible_with": []},
    {"name": "raw_path", "required": False, "warning": None, "incompatible_with": ["freshness_limit"]},
    {"name": "row_format", "required": False, "warning": None, "incompatible_with": []},
    {"name": "select", "required": False, "warning": None, "incompatible_with": []},
    {"name": "start_date", "required": False, "warning": None, "incompatible_with": []},
    {"name": "thread_count", "required": False, "warning": "Threaded Reader is Beta - use in production systems is not recommended", "incompatible_with": []},
    {"name": "query", "required": False, "warning": "", "incompatible_with": ["filters"]},
    {"name": "persistence", "required": False, "warning": "", "incompatible_with": []}

]
# fmt:on


@validate(RULES)
def Reader(
    *,  # force all paramters to be keyworded
    select: list = ["*"],
    dataset: str = None,
    filters: Optional[List[Tuple[str, str, object]]] = None,
    query: Optional[str] = None,
    inner_reader=None,  # type:ignore
    row_format: str = "json",
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
        filters: List of tuples (optional):
            Rows which do not match the filter predicate will be removed from
            scanned data. Default is no filtering.
            Each tuple has format: (`key`, `op`, `value`) and compares the key with
            the value. The supported op are: `=` or `==`, `!=`,  `<`, `>`, `<=`,
            `>=`, `in`, `!in` (not in), `contains` and `!contains` (doesn't
            contain) and `like`. If the `op` is `in` or `!in`, the `value` must be
            a _list_. `like` performs similar to the SQL operator `%` is a
            multicharacter wildcard and `_` is a single character wildcard.
            If a field is indexed, it will be used only for '==' and 'in' and
            'contains' operations.
        inner_reader: BaseReader (optional):
            The reader class to perform the data access Operators, the default is
            GoogleCloudStorageReader
        row_format: string (optional):
            Controls how the data is interpretted. 'json' will parse to a
            dictionary before _select_ or _where_ is applied, 'text' will just
            return the line that has been read, 'block' will return the content of
            a file as a record. the default is 'json'.
        start_date: datetime (optional):
            The starting date of the range to read over, default is today
        end_date: datetime (optional):
            The end date of the range to read over, default is today
        thread_count: integer (optional):
            **BETA**
            Use multiple threads to read data files, the default is to not use
            additional threads, the maximum number of threads is 8
        freshness_limit: string (optional):
            a time delta string (e.g. 6h30m = 6hours and 30 minutes) which
            incidates the maximum age of a dataset before it is no longer
            considered fresh. Where the 'time' of a dataset cannot be
            determined, it will be treated as midnight (00:00) for the date.
        fork_processes: boolean (optional):
            Create parallel processes to read data files. This may loose records if
            the records are not read from te reader fast enough. Using persistance to
            read all of the records will prevent this.
        persistence: STORAGE_CLASS (optional)
            How to cache the results, the default is NO_PERSISTANCE which will almost
            always return a generator. MEMORY should only be used where the dataset
            isn't huge and DISK is many times slower than MEMORY.
        cursor: dictionary (or string)
            Resume read from a given point (assumes other parameters are the same).
            If a JSON string is provided, it will converted to a dictionary.
        cache_indexes: boolean
            Cache indexes to speed up secondary access, default is False
            (do not use cache)

    Returns:
        DictSet

    Raises:
        TypeError
            Reader _select_ parameter must be a list
        TypeError
            Data format unsupported
        InvalidCombinationError
            Forking and Threading can not be used at the same time
    """
    if not isinstance(select, list):  # pragma: no cover
        raise TypeError("Reader 'select' parameter must be a list")

    # load the line converter
    parser = PARSERS.get(row_format.lower())
    if parser is None:  # pragma: no cover
        raise TypeError(
            f"Row format unsupported: {row_format} - valid options are {list(PARSERS.keys())}."
        )

    # lazy loading of dependency
    if inner_reader is None:
        from ...adapters.google import GoogleCloudStorageReader

        inner_reader = GoogleCloudStorageReader
    # instantiate the injected reader class
    reader_class = inner_reader(dataset=dataset, **kwargs)  # type:ignore

    cursor = kwargs.get("cursor", None)
    if isinstance(cursor, str):
        cursor = orjson.loads(cursor)
    if not isinstance(cursor, dict):
        cursor = {}

    select = select.copy()

    # index caching
    cache_folder = None
    if kwargs.get("cache_indexes", False):
        # saving in the environment allows us to reuse the cache across readers
        # in the same execution
        cache_folder = os.environ.get("CACHE_FOLDER")
        if not cache_folder:
            import tempfile

            cache_folder = (
                tempfile.TemporaryDirectory(prefix="mabel_cache-").name + os.sep
            )
            os.environ["CACHE_FOLDER"] = cache_folder
            os.makedirs(cache_folder, exist_ok=True)
            # delete the cache when the application closes
            atexit.register(shutil.rmtree, cache_folder, ignore_errors=True)

    arg_dict = kwargs.copy()
    arg_dict["select"] = f"{select}"
    arg_dict["dataset"] = f"{dataset}"
    arg_dict["inner_reader"] = f"{inner_reader.__name__}"  # type:ignore
    arg_dict["row_format"] = f"{row_format}"
    arg_dict["cache_folder"] = cache_folder
    arg_dict["query"] = query
    get_logger().debug(arg_dict)

    # number of days to walk backwards to find records
    freshness_limit = parse_delta(kwargs.get("freshness_limit", ""))

    if (
        freshness_limit and reader_class.start_date != reader_class.end_date
    ):  # pragma: no cover
        raise InvalidCombinationError(
            "freshness_limit can only be used when the start and end dates are the same"
        )

    if (
        row_format != "pass-thru" and kwargs.get("extension") == ".parquet"
    ):  # pragma: no cover
        raise InvalidCombinationError(
            "`parquet` extension much be used with the `pass-thru` row_format"
        )

    indexable_fields = []
    if filters:
        filters = DnfFilters(filters)  # type:ignore
        indexable_fields = get_indexable_filter_columns(
            filters.predicates  # type:ignore
        )  # type:ignore

    # threaded reader
    thread_count = kwargs.get("thread_count")
    if thread_count:
        thread_count = int(thread_count)
    else:
        thread_count = 0

    # multiprocessed reader
    fork_processes = bool(kwargs.get("fork_processes", False))

    return DictSet(
        _LowLevelReader(
            indexable_fields=indexable_fields,
            cache_folder=cache_folder,
            reader_class=reader_class,
            parser=parser,
            filters=filters,
            freshness_limit=freshness_limit,
            cursor=cursor,
            thread_count=thread_count,
            fork_processes=fork_processes,
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
        indexable_fields,
        cache_folder,
        reader_class,
        parser,
        filters,
        freshness_limit,
        cursor,
        thread_count,
        fork_processes,
        select,
        query,
    ):
        self.indexable_fields = indexable_fields
        self.cache_folder = cache_folder
        self.reader_class = reader_class
        self.parser = parser
        self.filters = filters
        self.freshness_limit = freshness_limit
        self.cursor = cursor
        self.thread_count = thread_count
        self.fork_processes = fork_processes
        self._inner_line_reader = None
        self.select = select
        self.query = query

    def _read_blob(self, blob, blob_list):
        """
        This wraps the blob reader, including the filters and indexers
        """
        # If an index exists, get the rows we're interested in from the index
        rows = None
        for field, filter_value in self.indexable_fields:
            # does an index file for this file and record exist

            bucket, path, stem, ext = get_parts(blob)
            index_file = f"{bucket}/{path}_SYS.{stem}.{safe_field_name(field)}.index"

            # TODO: index should only be used on all AND filters, no ORs

            if index_file in blob_list:
                # if we have a cache folder, we're using the cache
                # we hash the filename to remove any unwanted characters
                hashed_index_file = abs(hash(index_file))
                cache_hit = False
                cache_file = f"{self.cache_folder}{hashed_index_file}.index"
                if self.cache_folder:
                    # if the cache file exists, read it
                    if os.path.exists(cache_file):
                        with open(cache_file, "rb") as cache:
                            index_stream = io.BytesIO(cache.read())
                            cache_hit = True
                            get_logger().debug(
                                f"Reading index from `{index_file}` (cache hit)"
                            )
                # if we didn't hit the cache, read the file
                if not cache_hit:
                    index_stream = self.reader_class.get_blob_stream(index_file)
                    get_logger().debug(
                        f"Reading index from `{index_file}` (cache miss)"
                    )
                    # if we missed and he have a cache folder, cache the file
                    if self.cache_folder:
                        with open(cache_file, "wb") as cache:
                            cache.write(index_stream.read())
                        index_stream.seek(0, 2)

                index = Index(index_stream)
                rows = rows or []
                rows += index.search(filter_value)

        # read the rows from the file
        yield from self.reader_class.get_records(blob, rows)


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

        # skip to the blob in the cursor
        if self.cursor.get("partition"):
            skipped = 0
            while len(readable_blobs) > 0:
                if cityhash.CityHash32(readable_blobs[0]) != self.cursor.get(
                    "partition"
                ):
                    readable_blobs.pop(0)
                    skipped += 1
                else:
                    get_logger().debug(
                        f"Reader found {len(readable_blobs)} sources to read data from after {skipped} jumped to get to cursor."
                    )
                    break
        elif len(readable_blobs) == 0:
            message = f"Reader found {len(readable_blobs)} sources to read data from in `{self.reader_class.dataset}`."
            get_logger().error(message)
            raise DataNotFoundError(message)
        else:
            get_logger().debug(
                f"Reader found {len(readable_blobs)} sources to read data from in `{self.reader_class.dataset}`."
            )

        if self.thread_count > 0:
            yield from threaded_reader(readable_blobs, blob_list, self)

        elif self.fork_processes and len(readable_blobs) > 4:
            yield from processed_reader(readable_blobs, self.reader_class, self.parser)

        else:
            offset = self.cursor.get("offset", 0)
            for blob in readable_blobs:
                self.cursor["partition"] = cityhash.CityHash32(blob)
                self.cursor["offset"] = -1
                local_reader = self._read_blob(blob, blob_list)
                if offset > 0:
                    for burn in range(offset):
                        next(local_reader, None)
                    for record_offset, record in enumerate(self.parser(local_reader)):
                        self.cursor["offset"] = offset + record_offset
                        yield record
                else:
                    for self.cursor["offset"], record in enumerate(self.parser(local_reader)):
                        yield record
                offset = 0
            # when we're done, the cursor shouldn't point anywhere
            self.cursor = {}

    def __iter__(self):
        return self

    def __next__(self):
        if self._inner_line_reader is None:
            line_reader = self._create_line_reader()

            # filter the rows, either with the filters or `dictset.select_from`
            if self.filters:
                self._inner_line_reader = self.filters.filter_dictset(line_reader)
            elif self.query:
                self._inner_line_reader = filter(Expression(self.query).evaluate, line_reader)
            else:
                self._inner_line_reader = line_reader

        # get the the next line from the reader
        record = self._inner_line_reader.__next__()
        if self.select != ["*"]:
            record = select_record_fields(record, self.select)
        return record
