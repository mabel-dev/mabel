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
from . import decompressors, parsers
from mabel.utils import paths
from mabel.data.internals.index import Index
from mabel.data.internals.expression import Expression
from mabel.data.internals.dnf_filters import DnfFilters


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
    ".idx": (empty_list, empty_list, EXTENSION_TYPE.INDEX),
}


def pass_thru(x):
    return x


def no_filter(x):
    return True


class ParallelReader:

    NOT_INDEXED = ""

    def __init__(
        self,
        reader,
        filters=no_filter,
        reducer=pass_thru,
        override_format=None,
        **kwargs,
    ):
        """

        Parameters:
            reader: callable
            filters: callable
            reducer: callable
            **kwargs: kwargs
        """

        # DNF form is a representation of logical expressions which it is easiest
        # to apply any indicies to - we can convert Expressions to DNF but we can't
        # convert functions to DNF.
        if isinstance(filters, DnfFilters):
            self.dnf_filter = filters
        elif isinstance(filters, Expression):
            self.dnf_filter = filters.to_dnf()
        else:
            self.dnf_filter = None

        # the reader gets the data from the storage platform e.g. read the file from
        # disk or download the file
        self.reader = reader

        # this is the filter of the collected data, this can be used
        # against more operators
        self.filters = filters

        # this is aggregation and reducers for the data
        self.reducer = reducer

        self.override_format = override_format

        if self.override_format:
            self.override_format = self.override_format.lower()
            if not self.override_format[0] == ".":
                self.override_format = "." + self.override_format

    def pre_filter(self, blob_name, index_files):
        """
        Select rows from the file based on the filters and indexes, this filters
        the data before we've even seen it. This is usually faster as it avoids
        reading files with no records and parsing data we don't want.

        Go over the tree, if the predicate is an operator we can filter on and
        the field in the predicate is indexed, we can apply a pre-filter.

        ORs are fatal if both sides can't be pre-evaluated.
        """

        PRE_FILTERABLE_OPERATORS = {"=", "==", "is", "in", "contains"}

        def _inner_prefilter(predicate):
            # No filter doesn't filter
            if predicate is None:  # pragma: no cover
                return None

            # If we have a tuple extract out the key, operator and value and do the evaluation
            if isinstance(predicate, tuple):
                key, operator, value = predicate
                if operator in PRE_FILTERABLE_OPERATORS:
                    # do I have an index for this field?
                    for index_file in [index_file for index_file in index_files if f".{key}." in index_file]:
                        print("I HAVE", index_file)
                        index = Index(self.reader.get_blob_stream(index_file))
                return self.NOT_INDEXED

            if isinstance(predicate, list):
                # Are all of the entries tuples? These are ANDed together.
                if all([isinstance(p, tuple) for p in predicate]):
                    evaluations = [_inner_prefilter(p) for p in predicate]

                # Are all of the entries lists? These are ORed together.
                # All of the elements in an OR need to be indexable for use to be
                # able to prefilter.
                if all([isinstance(p, list) for p in predicate]):
                    evaluations = [_inner_prefilter(p) for p in predicate]
                    if not all([e == self.NOT_INDEXED for e in evaluations]):
                        return self.NOT_INDEXED
                    else:
                        pass

                # if we're here the structure of the filter is wrong
                return self.NOT_INDEXED
            return self.NOT_INDEXED

        print(_inner_prefilter(self.dnf_filter.predicates))
        return False, []

    def __call__(self, blob_name, index_files):

        try:
            if self.override_format:
                ext = self.override_format
            else:
                bucket, path, stem, ext = paths.get_parts(blob_name)

            if ext not in KNOWN_EXTENSIONS:
                return []
            decompressor, parser, file_type = KNOWN_EXTENSIONS[ext]

            # Pre-Filter
            if self.dnf_filter and len(index_files):
                (row_selector, selected_rows) = self.pre_filter(blob_name, index_files)
            else:
                row_selector = False
                selected_rows = []
            # Read
            record_iterator = self.reader.get_blob_stream(blob_name)
            # Decompress
            record_iterator = decompressor(record_iterator)
            ### bypass rows which aren't selected
            if row_selector:
                record_iterator = [record for index, record in enumerate(record_iterator) if index in selected_rows]
            # Parse
            record_iterator = map(parser, record_iterator)
            # Filter
            record_iterator = filter(self.filters, record_iterator)
            # Reduce
            record_iterator = self.reducer(record_iterator)
            # Yield
            yield from record_iterator
        except Exception as e:
            import traceback
            print(f"{blob_name} had an error - {e}\n{traceback.format_exc()}")
            return []
