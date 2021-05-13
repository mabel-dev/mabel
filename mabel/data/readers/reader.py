import sys
import os.path
import threading
from typing import Callable, Optional, Tuple, List
from .internals.threaded_reader import threaded_reader
from .internals.alpha_processed_reader import processed_reader
from .internals.parsers import pass_thru_parser, block_parser, json_parser
from .internals.filters import Filters
from ..formats.dictset import select_record_fields, select_from
from ..formats.dictset.display import html_table, ascii_table
from ..formats import json
from ...logging import get_logger
from ...errors import InvalidCombinationError, MissingDependencyError
from ...index.index import Index, safe_field_name
from ...utils.parameter_validator import validate


# available parsers
PARSERS = {
    "json": json_parser,
    "text": pass_thru_parser,
    "block": block_parser,
    "pass-thru": pass_thru_parser
}

RULES = [
    {"name": "as_at", "required": False, "warning": "Time Travel (as_at) is Alpha - it's interface may change and some features may not be supported", "incompatible_with": ['start_date', 'end_date']},
    {"name": "dataset", "required": True, "warning": None, "incompatible_with": []},
    {"name": "end_date", "required": False, "warning": None, "incompatible_with": []},
    {"name": "extension", "required": False, "warning": None, "incompatible_with": []},
    {"name": "filters", "required": False, "warning": None, "incompatible_with": []},
    {"name": "fork_processes", "required": False, "warning": "Forked Reader is Alpha - it's interface may change and some features may not be supported", "incompatible_with": ['thread_count']},
    {"name": "from_path", "required": False, "warning": "DEPRECATION: Reader \'from_path\' parameter will be replaced with \'dataset\'", "incompatible_with": ['dataset']},
    {"name": "inner_reader", "required": False, "warning" :None, "incompatible_with": []},
    {"name": "project", "required": False, "warning": None, "incompatible_with": []},
    {"name": "raw_path", "required": False, "warning": None, "incompatible_with": []},
    {"name": "row_format", "required": False, "warning": None, "incompatible_with": []},
    {"name": "select", "required": False, "warning": None, "incompatible_with": []},
    {"name": "self", "required": True, "warning": None, "incompatible_with": []},
    {"name": "start_date", "required": False, "warning": None, "incompatible_with": []},
    {"name": "step_back_days", "required": False, "warning": None, "incompatible_with": []},
    {"name": "thread_count", "required": False, "warning": "Threaded Reader is Beta - use in production systems is not recommended", "incompatible_with": []},
    {"name": "where", "required": False, "warning": "`where` will be deprecated, use `filters` or `dictset.select_from` instead", "incompatible_with": ['filters']},
]


class Reader():

    @validate(RULES)
    def __init__(
        self,
        *,  # force all paramters to be keyworded
        select: list = ['*'],
        dataset: str = None,
        where: Optional[Callable] = None,
        filters: Optional[List[Tuple[str, str, object]]] = None,
        inner_reader=None,   # type:ignore
        row_format: str = "json",
        **kwargs):
        """
        Reads records from a data store, opinionated toward Google Cloud
        Storage but a filesystem reader is available to assist with local
        development.

        The Reader will iterate over a set of files and return them to the
        caller as a single stream of records. The files can be read from a
        single folder or can be matched over a set of date/time formatted
        folder names. This is useful to read over a set of logs. The date range
        is provided as part of the call; this is essentially a way to partition
        the data by date/time.

        The reader can filter records to return a subset, for JSON formatted
        data the records can be converted to dictionaries before filtering.
        JSON data can also be used to select columns, so not all read data is
        returned.

        The reader does not support aggregations, calculations or grouping of
        data, it is a log reader and returns log entries. The reader can
        convert a set into _Pandas_ dataframe, or the _dictset_ helper library
        can perform some activities on the set in a more memory efficient
        manner.

        Note:
            Different _inner_readers_ may take or require additional
            parameters.

        Parameters:
            select: list of strings (optional):
                A list of the names of the columns to return from the dataset,
                the default is all columns
            dataset: string:
                The path to the data
            where: callable (optional):
                **TO BE DEPRECATED**
                A method (function or lambda expression) to filter the returned
                records, where the function returns True the record is
                returned, False the record is skipped. The default is all
                records
            filters: List of tuples (optional)
                Rows which do not match the filter predicate will be removed
                from scanned data. Default is no filtering.
                Each tuple has format: (`key`, `op`, `value`) and compares the
                key with the value. The supported op are: `=` or `==`, `!=`, 
                `<`, `>`, `<=`, `>=`, `in`, `!in` (not in)  and `like`. If the
                 `op` is `in` or `!in`, the `value` must be a collection such
                as a _list_, a _set_ or a _tuple_.
                `like` performs similar to the SQL operator `%` is a
                multicharacter wildcard and `_` is a single character wildcard.
                If a field is indexed, it will be used only for '==' and
                'contains' operations.
            inner_reader: BaseReader (optional):
                The reader class to perform the data access Operators, the
                default is GoogleCloudStorageReader
            row_format: string (optional):
                Controls how the data is interpretted. 'json' will parse to a
                dictionary before _select_ or _where_ is applied, 'text' will
                just return the line that has been read, 'block' will return
                the content of a file as a record. the default is 'json'.
            start_date: datetime (optional):
                The starting date of the range to read over - if used with
                _date_range_, this value will be preferred, default is today
            end_date: datetime (optional):
                The end date of the range to read over - if used with
                _date_range_, this value will be preferred, default is today
            thread_count: integer (optional):
                **BETA**
                Use multiple threads to read data files, the default is to not
                use additional threads, the maximum number of threads is 8
            step_back_days: integer (optional):
                The number of days to look back if data for the current date is
                not available.
            fork_processes: boolean (alpha):
                **ALPHA**
                Create parallel processes to read data files
            as_at: datetime (alpha)
                **ALPHA**
                Time travel

        Yields:
            dictionary (string if data format is 'text')

        Raises:
            TypeError
                Reader _select_ parameter must be a list
            TypeError
                Reader _where_ parameter must be Callable or None
            TypeError
                Data format unsupported
            InvalidCombinationError
                Forking and Threading can not be used at the same time
        """
        if not isinstance(select, list):  # pragma: no cover
            raise TypeError("Reader 'select' parameter must be a list")
        if where is not None and not hasattr(where, '__call__'):
            raise TypeError("Reader 'where' parameter must be Callable or None")

        # load the line converter
        self._parse = PARSERS.get(row_format.lower())
        if self._parse is None:  # pragma: no cover
            raise TypeError(F"Row format unsupported: {row_format} - valid options are {list(PARSERS.keys())}.")

        if dataset is None and kwargs.get('from_path'):  # pragma: no cover
            dataset = kwargs['from_path']

        # lazy loading of dependency
        if inner_reader is None:
            from ...adapters.google import GoogleCloudStorageReader
            inner_reader = GoogleCloudStorageReader
        # instantiate the injected reader class
        self.reader_class = inner_reader(dataset=dataset, **kwargs)  # type:ignore

        self.select = select.copy()
        self.where: Optional[Callable] = where

        # initialize the reader
        self._inner_line_reader = None

        arg_dict = kwargs.copy()
        arg_dict['select'] = F'{select}'
        arg_dict['dataset'] = F'{dataset}'
        arg_dict['inner_reader'] = F'{inner_reader.__name__}'  # type:ignore
        arg_dict['row_format'] = F'{row_format}'
        get_logger().debug(json.serialize(arg_dict))

        # number of days to walk backwards to find records
        self.step_back_days = int(kwargs.get('step_back_days', 0))
        if self.step_back_days > 0 and self.reader_class.start_date != self.reader_class.end_date:  # pragma: no cover
            raise InvalidCombinationError("step_back_days can only be used when the start and end dates are the same")

        if row_format != 'pass-thru' and kwargs.get('extension') == '.parquet':  # pragma: no cover
            raise InvalidCombinationError("`parquet` extension much be used with the `pass-thru` row_format")

        self.filters = None
        self.indexable_fields = []
        if filters:
            self.filters = Filters(filters)   
            self.indexable_fields = self._get_indexable_filter_columns(self.filters.predicates)

        """ FEATURES IN DEVELOPMENT """

        # threaded reader
        self.thread_count = int(kwargs.get('thread_count', 0))

        # multiprocessed reader
        self.fork_processes = bool(kwargs.get('fork_processes', False))

        # time travel
        self.as_at = kwargs.get('as_at')

        
    """
    Iterable

    Use this class as an iterable:

        for line in Reader("file"):
            print(line)
    """
    def _get_indexable_filter_columns(self, predicate):
        """
        Returns all of the columns in a filter which the operation benefits
        from an index

        This creates an list of tuples of (field,value) that we can feed to the
        index search.
        """
        INDEXABLE_OPS = {'=', '==', 'is', 'in', 'contains'}
        if predicate is None:
            return []
        if isinstance(predicate, tuple):
            key, op, value = predicate
            if op in INDEXABLE_OPS:
                return [(key, value,)]
        if isinstance(predicate, list):
            if all([isinstance(p, tuple) for p in predicate]):
                return [(k,v,) for k,o,v in predicate if o in INDEXABLE_OPS]
            if all([isinstance(p, list) for p in predicate]):
                columns = []
                for p in predicate:
                    columns += self._get_indexable_filter_columns(p)
                return columns
        return []    # pragma: no cover

    def _is_system_file(self, filename):
        if '_SYS.' in filename:
            base = os.path.basename(filename)
            return base.startswith('_SYS.')
        return False

    def _read_blob(self, blob, blob_list):
        """
        This wraps the blob reader, including the filters and indexers
        """
        get_logger().debug(F"Reading from `{blob}`, thread: {threading.get_ident()}")
        # If an index exists, get the rows we're interested in from the index
        rows = None
        for field, filter_value in self.indexable_fields:
            # does an index file for this file and record exist
            path, file = os.path.split(blob)
            stem, ext = os.path.splitext(file)
            index_file = path + '/_SYS.' + stem + '.' + safe_field_name(field) + '.index'

            if index_file in blob_list:
                get_logger().debug(F"Reading index from blob `{index_file}`")
                index_stream = self.reader_class.get_blob_stream(index_file)
                index = Index(index_stream)
                rows = rows or []
                rows += index.search(filter_value)

        # read the rows from the file
        ds = self.reader_class.get_records(blob, rows)
        # reformat the rows
        ds = self._parse(ds)
        # filter the rows, either with the filters or `dictset.select_from`
        if self.filters:
            yield from self.filters.filter_dictset(ds)
        else:
            yield from select_from(ds, where=self.where)

    def _create_line_reader(self):
        blob_list = self.reader_class.get_list_of_blobs()

        # handle stepping back if the option is set
        if self.step_back_days > 0:
            while not bool(blob_list) and self.step_back_days >= self.reader_class.days_stepped_back:
                self.reader_class.step_back_a_day()
                blob_list = self.reader_class.get_list_of_blobs()
            if self.step_back_days < self.reader_class.days_stepped_back:
                get_logger().alert(F"No data found in last {self.step_back_days} days - aborting")
                sys.exit(-1) 
            if self.reader_class.days_stepped_back > 0:
                get_logger().warning(F"Stepped back {self.reader_class.days_stepped_back} days to {self.reader_class.start_date} to find last data, my limit is {self.step_back_days} days.")

        readable_blobs = [b for b in blob_list if not self._is_system_file(b)]
        get_logger().debug(F"Reader found {len(readable_blobs)} sources to read data from.")
        
        if self.thread_count > 0:
            yield from threaded_reader(readable_blobs, blob_list, self)

        elif self.fork_processes:
            yield from processed_reader(readable_blobs, self.reader_class, self._parse, self.where)

        else:
            for blob in readable_blobs:
                yield from self._read_blob(blob, blob_list)

    def __iter__(self):
        return self

    def __next__(self):
        if self._inner_line_reader is None:
            self._inner_line_reader = self._create_line_reader()

        # get the the next line from the reader
        record = self._inner_line_reader.__next__()
        if self.select != ['*']:
            record = select_record_fields(record, self.select)
        return record


    """
    Context Manager

    Use this class using the 'with' statement:

        with Reader("file") as r:
            line = r.read_line()
            while line:
                print(line)
    """

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass  # exist needs to exist to be a context manager

    def read_line(self):
        """
        Read the next line from the _Reader_.

        Returns:
            dictionary (or string)
        """
        try:
            return self.__next__()
        except StopIteration:
            return None

    """
    Exports
    """
    def to_pandas(self):
        """
        Load the contents of the _Reader_ to a _Pandas_ DataFrame.

        Returns:
            Pandas DataFrame
        """
        try:
            import pandas as pd  # type:ignore
        except ImportError:  # pragma: no cover
            raise MissingDependencyError("`pands` is missing, pleaseinstall or include in requirements.txt")
        return pd.DataFrame(self)

    def __repr__(self):  # pragma: no cover

        def is_running_from_ipython():
            """
            True when running in Jupyter
            """
            try:
                from IPython import get_ipython  # type:ignore
                return get_ipython() is not None
            except Exception:
                return False

        if is_running_from_ipython():
            from IPython.display import HTML, display  # type:ignore
            html = html_table(self, 5)
            display(HTML(html))
            return ''  # __repr__ must return something
        else:
            return ascii_table(self, 5)
