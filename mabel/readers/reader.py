"""
Reader

Reads records from a data store, opinionated toward Google Cloud Storage
but a filesystem reader was created primarily to assist with development.

The reader will iterate over a set of files and return them to the caller
as a single stream of records. The files can be read from a single folder
or can be matched over a set of date/time formatted folder names. This is
useful to read over a set of logs. The date range is provided as part of 
the call; this is essentially a way to partition the data by date/time.

The reader can filter records to return a subset, for json formatted data
the records can be converted to dictionaries before filtering. json data
can also be used to select columns, so not all read data is returned.

The reader does not support aggregations, calculations or grouping of data,
it is a log reader and returns log entries. The reader can convert a set
into Pandas dataframe, or the dictset helper library can perform some 
activities on the set in a more memory efficient manner.
"""
from typing import Callable, Tuple, Optional
from ..dictset import select_all, select_record_fields
from .blob_reader import blob_reader
import xmltodict  # type:ignore
import logging
import datetime
import json
json_parser: Callable = json.loads
json_dumper: Callable = json.dumps
try:
    import orjson
    json_parser = orjson.loads
    json_dumper = orjson.dumps
except ImportError:
    pass
try:
    import ujson
    json_parser = ujson.loads
except ImportError:
    pass

logger = logging.getLogger("MBL")

FORMATTERS = {
    "json": json_parser,
    "text": lambda x: x,
    "xml": lambda x: xmltodict.parse(x)
}


class Reader():

    def __init__(
        self,
        select: list = ['*'],
        from_path: str = None,
        where: Callable = select_all,
        limit: int = -1,
        reader: Callable = blob_reader,
        data_format: str = "json",
        date_range: Tuple[Optional[datetime.date], Optional[datetime.date]] = (None, None),
        cursor: str = '',  # __
        **kwargs):
        """
        Reader accepts a method which iterates over a data source and provides
        functionality to filter, select and truncate records which are
        returned. The default reader is a GCS blob reader, a file system
        reader is also implemented.

        Reader roughly follows a SQL Select:

        SELECT column FROM data.store WHERE size == 'large'

        Reader(
            select=['column'],
            from_path='data/store',
            where=lambda record: record['size'] == 'large',
            limit=1
        )

        It's the data is automatically partitioned by date.
        """
        self.reader = reader(path=from_path, date_range=date_range, **kwargs)
        self.format = data_format
        self.formatter = FORMATTERS.get(self.format.lower())
        if not self.formatter:
            raise TypeError(F"data format unsupported: {self.format}.")
        self.select = select.copy()
        self.where: Callable = where
        self.limit: int = limit

        logger.debug(F"Reader(reader={reader.__name__}, from_path='{from_path}', date_range={date_range})")

    """
    Iterable

    Use this class as an iterable:

        for line in Reader("file"):
            print(line)
    """
    def __iter__(self):
        self.seen_hashs = {}
        return self

    def __next__(self):
        """
        This wraps the primary filter and select logic
        """
        if self.limit == 0:
            raise StopIteration()
        self.limit -= 1
        while True:
            record = self.reader.__next__()
            record = self.formatter(record)
            if not self.where(record):
                continue
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
        try:
            return self.__next__()
        except StopIteration:
            return None

    """
    Exports
    """
    def to_pandas(self):
        """
        Only import Pandas if needed
        """
        try:
            import pandas as pd  # type:ignore
        except ImportError:
            raise ImportError("Pandas must be installed to use 'to_pandas'")
        return pd.DataFrame(self)

        """
        cursor = {
            select: list = ['*'],
            from_path: str = None,
            where: Callable = select_all,
            limit: int = -1,
            reader: Callable = blob_reader,
            data_format: str = "json",
            date_range: Tuple[datetime.date] = (None, None),

            current_file:
            current_index:
        }
        """
