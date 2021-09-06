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
from . import decompressors, parsers
from ....utils import paths


def empty_list(x):
    return []


VALID_EXTENSIONS = {
    ".txt": (decompressors.block, parsers.pass_thru),
    ".json": (decompressors.block, parsers.json),
    ".zstd": (decompressors.zstd, parsers.json),
    ".lzma": (decompressors.lzma, parsers.json),
    ".zip": (decompressors.unzip, parsers.json),
    ".jsonl": (decompressors.lines, parsers.json),
    ".xml": (decompressors.block, parsers.xml),
    ".lxml": (decompressors.lines, parsers.xml),
    ".parquet": (decompressors.parquet, parsers.pass_thru),
    ".csv": (decompressors.csv, parsers.pass_thru),
    ".ignore": (empty_list, empty_list),
    ".idx": (empty_list, empty_list),
    ".complete": (empty_list, empty_list),
}


def pass_thru(x):
    return x


def no_filter(x):
    return True


class ParallelReader:

    NOT_INDEXED = 1

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

        # this is the representation of the filters in a which makes it
        # easier to use indexes - but can only really be used against
        # a subset of the operators
        # self.dnf_filter

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

    def pre_filter(self):
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
                    if key in []:
                        pass
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
                raise InvalidSyntaxError(
                    "Unable to evaluate Filter"
                )  # pragma: no cover

            raise InvalidSyntaxError("Unable to evaluate Filter")  # pragma: no cover

        return _inner_prefilter(self.dnf_fiter)

    def select_rows(self, records, rows):
        """
        Apply the prefilter
        """
        for index, record in records:
            if index in rows:
                yield record

    def pass_thru_print(self, records):
        for record in records:
            print(record)
            yield record

    def __call__(self, blob_name):

        try:
            if self.override_format:
                ext = self.override_format
            else:
                bucket, path, stem, ext = paths.get_parts(blob_name)

            if ext not in VALID_EXTENSIONS:
                return []
            decompressor, parser = VALID_EXTENSIONS[ext]

            # Pre-Filter
            # (row_selector, rows) = self.pre_filter(self.dnf_filters)
            row_selector = False
            # Read
            record_iterator = self.reader.get_blob_stream(blob_name)
            # Decompress
            record_iterator = decompressor(record_iterator)
            ### bypass rows which aren't selected
            if row_selector:
                record_iterator = record_iterator
            # Parse
            record_iterator = map(parser, record_iterator)
            # Filter
            record_iterator = filter(self.filters, record_iterator)
            # Reduce
            record_iterator = self.reducer(record_iterator)
            # Yield
            return list(record_iterator)
        except Exception as e:
            print(f"{blob_name} had an error - {e}")
            return []
