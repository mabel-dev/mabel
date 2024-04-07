# cython: language_level=3
"""
This is an contained reading pipeline, intended to be run in a thread/processes.

The processing pipeline is made of a set of steps operating on a defined and finite
dataset:

┌────────────┬────────────────────────────────────────────────────────────┐
│ Function   │ Role                                                       │
├────────────┼────────────────────────────────────────────────────────────┤
│ Pre-Filter │ Pre-filter the rows based on a subset of the filters       │
│            │                                                            │
│ Read       │ Read the raw content from the file                         │
│            │                                                            │
│ Decompress │ Convert the raw content to lined data                      │
│            │                                                            │
│ Parse      │ Interpret the lined data into dictionaries                 │
│            │                                                            │
│ Filter     │ Apply full set of row filters to the read data             │
│            │                                                            │
│ Reduce     │ Aggregate                                                  │
└────────────┴────────────────────────────────────────────────────────────┘
"""
from enum import Enum

from orso import logging

from mabel.data.internals.dnf_filters import DnfFilters
from mabel.data.internals.expression import Expression
from mabel.data.internals.records import flatten
from mabel.utils import paths

from . import decompressors
from . import parsers

logger = logging.get_logger()


def empty_list(x):
    return []


class EXTENSION_TYPE(str, Enum):
    # labels for the file extentions
    DATA = "DATA"
    CONTROL = "CONTROL"
    INDEX = "INDEX"


KNOWN_EXTENSIONS = {
    ".txt": (decompressors.block, parsers.pass_thru, EXTENSION_TYPE.DATA),
    ".json": (decompressors.block, parsers.json, EXTENSION_TYPE.DATA),
    ".zstd": (decompressors.zstd, parsers.json, EXTENSION_TYPE.DATA),
    ".lzma": (decompressors.lzma, parsers.json, EXTENSION_TYPE.DATA),
    ".zip": (decompressors.unzip, parsers.pass_thru, EXTENSION_TYPE.DATA),
    ".jsonl": (decompressors.lines, parsers.json, EXTENSION_TYPE.DATA),
    ".xml": (decompressors.block, parsers.xml, EXTENSION_TYPE.DATA),
    ".lxml": (decompressors.lines, parsers.xml, EXTENSION_TYPE.DATA),
    ".parquet": (decompressors.parquet, parsers.pass_thru, EXTENSION_TYPE.DATA),
    ".csv": (decompressors.csv, parsers.pass_thru, EXTENSION_TYPE.DATA),
    ".ignore": (empty_list, empty_list, EXTENSION_TYPE.CONTROL),
    ".complete": (empty_list, empty_list, EXTENSION_TYPE.CONTROL),
    ".manifest": (empty_list, empty_list, EXTENSION_TYPE.CONTROL),
    ".idx": (empty_list, empty_list, EXTENSION_TYPE.INDEX),
}


pass_thru = lambda x: x


def no_filter(x):
    return True


def expand_nested_json(row):
    # this is really slow - on a simple read it's roughly 60% of the execution
    if hasattr(row, "items"):
        for k, v in [(k, v) for k, v in row.items()]:
            if hasattr(v, "items"):
                if hasattr(row, "as_dict"):
                    row = row.as_dict()  # only convert if we really have to
                row.update(flatten(dictionary={k: v}, separator="."))
                row.pop(k)
    return row


class ParallelReader:
    NOT_INDEXED = {-1}

    def __init__(
        self,
        reader,
        filters=no_filter,
        columns="*",
        reducer=pass_thru,
        override_format=None,
        **kwargs,
    ):
        """

        Parameters:
            reader: callable
            columns: callable
            filters: callable
            reducer: callable
            **kwargs: kwargs
        """

        # DNF form is a representation of logical expressions which it is easier
        # to apply any indicies to - we can convert Expressions to DNF but we can't
        # convert functions to DNF.
        if isinstance(filters, DnfFilters):
            self.dnf_filter = filters
        elif isinstance(filters, Expression):
            self.dnf_filter = DnfFilters(filters.to_dnf())
        else:
            self.dnf_filter = None

        # the reader gets the data from the storage platform e.g. read the file from
        # disk or download the file
        self.reader = reader

        self.columns = columns

        # this is the filter of the collected data, this can be used
        # against more operators
        self.filters = filters

        # this is aggregation and reducers for the data
        self.reducer = reducer

        # sometimes the user knows better
        self.override_format = override_format

        if self.override_format:
            self.override_format = self.override_format.lower()
            if not self.override_format[0] == ".":
                self.override_format = "." + self.override_format

    def __call__(self, blob_name, index_files):
        # print(blob_name, "in")
        try:
            if self.override_format:
                ext = self.override_format
            else:
                bucket, path, stem, ext = paths.get_parts(blob_name)

            if ext not in KNOWN_EXTENSIONS:
                return []
            decompressor, parser, file_type = KNOWN_EXTENSIONS[ext]

            # Read
            record_iterator = self.reader.read_blob(blob_name)
            # Decompress
            record_iterator = decompressor(record_iterator)
            # Parse
            record_iterator = map(parser, record_iterator)
            # Expand Nested JSON
            # record_iterator = map(expand_nested_json, record_iterator)
            # Transform
            record_iterator = map(self.columns, record_iterator)
            # Filter
            record_iterator = filter(self.filters, record_iterator)
            # Reduce
            record_iterator = self.reducer(record_iterator)
            # Yield
            yield from record_iterator
            # print(blob_name, "out")
        except Exception as e:
            import traceback

            logger.error(f"{blob_name} had an error - {e}\n{traceback.format_exc()}")
            return []
