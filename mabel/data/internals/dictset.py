# no-maintain-checks
"""
DICT(IONARY) (DATA)SET

A class creating a Data Frame type construct with lists of dictionaries.

(C) 2021 Justin Joyce.

https://github.com/joocer

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import os
import orjson
import statistics

import simdjson

from siphashc import siphash
from operator import itemgetter
from functools import reduce

from typing import Iterable, Dict, Any, List, Union

# from ....logging import get_logger
from ...errors import MissingDependencyError, InvalidArgument
from ...utils.ipython import is_running_from_ipython

from .display import html_table, ascii_table
from .storage_classes import (
    StorageClassMemory,
    StorageClassDisk,
    StorageClassCompressedMemory,
)
from .expression import Expression
from .dnf_filters import DnfFilters
from .dumb_iterator import DumbIterator
from .group_by import GroupBy

from enum import Enum


class STORAGE_CLASS(int, Enum):
    """
    How to cache the results for processing:

    - NO_PERSISTANCE = don't do anything with the records to cache them, assumes
      the records are already persisted (e.g. you've loaded a list) but most
      functionality works with generators.
    - MEMORY = load the entire dataset into a list, this is fast but if the
      dataset is too large it will kill the app.
    - DISK = load the entire dataset to a temporary file, this can deal with
      Tb of data (if you have that much disk space) but it is at least 3x slower
      than memory from basic bench marks.
    - COMPRESSED_MEMORY = a middle ground, allows you to load more data into
      memory but still needs to perform compression on the data so isn't as fast
      as the MEMORY option. Bench marks show you can fit about 2x the data in
      memory but at a cost of 2.5x - your results will vary.
    """

    NO_PERSISTANCE = 1
    MEMORY = 2
    DISK = 3
    COMPRESSED_MEMORY = 4


class DictSet(object):
    def __init__(
        self,
        iterator: Iterable[Dict[Any, Any]],
        *,
        storage_class=STORAGE_CLASS.NO_PERSISTANCE,
    ):
        """
        Create a DictSet.

        Parameters:
            iterator: Iterable
                An iterable which is our DictSet
            persistance: STORAGE_CLASS (optional)
                How to store this dataset while we're processing it. The default is
                NO_PERSISTANCE which applies no specific persistance. MEMORY loads
                into a Python `list`, DISK saves to disk - disk persistance is slower
                but can handle much larger data sets. 'COMPRESSED_MEMORY' uses
                compression to fit more in memory for a performance cost.
        """
        self.storage_class = storage_class
        self._iterator = iterator
        self._temporary_folder = None

        # if we're persisting to memory, load into a list
        if storage_class == STORAGE_CLASS.MEMORY:
            self._iterator = StorageClassMemory(iterator)  # type:ignore

        # if we're persisting to disk, save it
        if storage_class == STORAGE_CLASS.DISK:
            self._iterator = StorageClassDisk(iterator)

        # if we're persisiting to compressed memory, do it
        if storage_class == STORAGE_CLASS.COMPRESSED_MEMORY:
            self._iterator = StorageClassCompressedMemory(iterator)

        if not hasattr(self._iterator, "__iter__"):
            self._iterator = DumbIterator(self._iterator)

    def __iter__(self):
        """
        Wrap the iterator in a Iterable object
        """
        if not hasattr(self._iterator, "__iter__"):
            self._iterator = DumbIterator(self._iterator)
        return self

    def __next__(self):
        return next(self._iterator)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass  # exist needs to exist to be a context manager

    def persist(self, storage_class=STORAGE_CLASS.MEMORY):
        """
        Persist changes the persistance engine used for the DictSet. The default
        is no defined persistance, we can persist to MEMORY, which is only really
        suitable for small datasets (or machines with large memories). The other
        option is DISK, this is approximately 10x slower than MEMORY.
        """
        if storage_class == STORAGE_CLASS.NO_PERSISTANCE:
            raise InvalidArgument("Persist cannot persist to 'NO_PERISISTANCE'")
        if self.storage_class == storage_class:
            return False
        if storage_class == STORAGE_CLASS.MEMORY:
            self._iterator == StorageClassMemory(self._iterator)
        if storage_class == STORAGE_CLASS.DISK:
            self._iterator = StorageClassDisk(self._iterator)
        if storage_class == STORAGE_CLASS.COMPRESSED_MEMORY:
            self._iterator = StorageClassCompressedMemory(self._iterator)
        self.storage_class = storage_class
        return True

    def sample(self, fraction: float = 0.5):
        """
        Select a random sample of records, fraction indicates the portion of
        records to select.

        NOTE: records are randomly selected so is unlikely to perfectly match the
        fraction.
        """

        def inner_sampler(dictset):
            selector = int(1 / fraction)
            for row in dictset:
                random_value = int.from_bytes(os.urandom(2), "big")
                if random_value % selector == 0:
                    yield row

        return DictSet(
            inner_sampler(iter(self._iterator)), storage_class=self.storage_class
        )

    def collect(self, key: str = None) -> Union[list, map]:
        """
        Convert a _DictSet_ to a list, optionally, but probably usually, just extract
        a specific column.

        Return None if the value in the field is None, if the field doesn't exist in
        the record, don't return anything.
        """
        if not key:
            return list(iter(self._iterator))
        return [record[key] for record in iter(self._iterator) if key in record]

    def keys(self, number_of_rows: int = 0):
        """
        Get all of the keys from the _DictSet_. This iterates the entire
        _DictSet_ unless told not to.
        """
        if number_of_rows > 0:
            rows = self.itake(number_of_rows)
            return reduce(
                lambda x, y: x + [a for a in y.keys() if a not in x], rows, []
            )
        return reduce(
            lambda x, y: x + [a for a in y.keys() if a not in x],
            iter(self._iterator),
            [],
        )

    def types(self, number_of_rows: int = 100):
        top = self.take(number_of_rows)
        response = {}
        for key in top.keys():
            key_type = {type(val).__name__ for val in top.collect(key) if val != None}
            if len(key_type) == 1:
                response[key] = key_type.pop()
            elif sorted(key_type) == ["float", "int"]:
                response[key] = "numeric"
            else:
                response[key] = "mixed"
        return response

    def max(self, key: str):
        """
        Find the maximum in a column of this _DictSet_.

        Parameters:
            key: string
                The column to perform the function on
        """
        return reduce(max, self.collect(key))

    def sum(self, key: str):
        """
        Find the sum of a column of this _DictSet_.

        Parameters:
            key: string
                The column to perform the function on
        """
        return reduce(lambda x, y: x + y, self.collect(key), 0)

    def min(self, key: str):
        """
        Find the minimum in a column of this _DictSet_.

        Parameters:
            key: string
                The column to perform the function on
        """
        return reduce(min, self.collect(key))

    def min_max(self, key: str):
        """
        Find the minimum and maximum of a column at the same time.

        Parameters:
            key: string
                The column to perform the function on

        Returns:
            tuple (minimum, maximum)
        """

        def minmax(a, b):
            return min(a[0], b[0]), max(a[1], b[1])

        return reduce(minmax, map(lambda x: (x, x), self.collect(key)))

    def mean(self, key: str):
        """
        Find the mean in a column of this _DictSet_.

        Parameters:
            key: string
                The column to perform the function on
        """
        return statistics.mean(self.collect(key))

    def variance(self, key: str):
        """
        Find the variance in a column of this _DictSet_.

        Parameters:
            key: string
                The column to perform the function on
        """
        return statistics.variance(self.collect(key))

    def standard_deviation(self, key: str):
        """
        Find the standard deviation in a column of this _DictSet_.

        Parameters:
            key: string
                The column to perform the function on
        """
        return statistics.stdev(self.collect(key))

    def count(self):
        """
        Count the number of items in the _DictSet_.
        """
        if hasattr(self._iterator, "__len__"):
            return len(self._iterator)
        else:
            # we can't count the items in an non persisted DictSet
            return -1

    def distinct(self, *columns):
        """
        Remove duplicates from a _DictSet_. This creates a list of the items
        already added to the result, so is not suitable for huge _DictSets_.

        Optionally accepts a list of columns, which we extract out and just
        'distinct' on these, ignoring differences in any of the other columns.
        """
        hash_list = {}

        def do_dedupe(data):
            for item in data:
                if columns:
                    hashed_item = hash(
                        "".join([str(item.get(c, "$$")) for c in columns])
                    )
                else:
                    # ensure the fields are in the same order
                    # this is quicker than dealing with dictionaries
                    hashed_item = hash(
                        "".join([f"{k}:{item[k]}" for k in sorted(item.keys())])
                    )
                if hashed_item not in hash_list:
                    yield item
                    hash_list[hashed_item] = True

        return DictSet(
            do_dedupe(iter(self._iterator)), storage_class=self.storage_class
        )

    def group_by(self, *group_by_column):
        """
        Group a dictset by a column or group of columns. Returns a GroupBy object.
        """
        return GroupBy(iter(self._iterator), *group_by_column)

    def get_items(self, *locations):
        """
        Get items from the DictSet at a set of indicies, try to find the fastest
        way possible to do this.
        """

        # if there's no direct access to items, cycle through them
        # yielding the items we want
        if self.storage_class in (STORAGE_CLASS.DISK, STORAGE_CLASS.COMPRESSED_MEMORY):
            for r in self._iterator._inner_reader(*locations):
                yield r
            return

        # if the iterator allows us to access items directly, use that
        if self.storage_class == STORAGE_CLASS.MEMORY or hasattr(
            self._iterator, "__getitem__"
        ):
            yield from [self._iterator[i] for i in locations]
            return

        if self.storage_class == STORAGE_CLASS.NO_PERSISTANCE:
            for i, r in iter(self._iterator):
                if i in locations:
                    yield r

    def to_ascii_table(self, limit: int = 5):
        """
        Return the top `limit` rows from a _DictSet_ as an ASCII table.

        Returns:
            Table encoded in a string
        """
        return ascii_table(iter(self._iterator), limit)

    def to_html_table(self, limit: int = 5):
        """
        Return the top `limit` rows from a _DictSet_ as a HTML table.

        Returns:
            HTML Table encoded in a string
        """
        return html_table(DumbIterator(self._iterator), limit)

    def to_pandas(self):
        """
        Load the contents of the _DictSet_ to a _Pandas_ DataFrame.

        Returns:
            Pandas DataFrame
        """
        try:
            import pandas
        except ImportError:  # pragma: no cover
            raise MissingDependencyError(
                "`pandas` is missing, please install or include in requirements.txt"
            )
        return pandas.DataFrame(iter(self._iterator))

    def first(self) -> dict:
        """
        Retun the first item in the DictSet
        """
        f = next(iter(self._iterator), None)
        if isinstance(f, simdjson.Object):
            return f.as_dict()
        return f

    def take(self, items: int):
        """
        Return the first _items_ number of items from the _DictSet_. This loads
        these items into memory. If returning a large number of items, use itake.
        """
        return DictSet(self.itake(items), storage_class=self.storage_class)

    def itake(self, items: int):
        """
        Return the first _items_ number of items from the _DictSet_.

        This returns a generator.
        """
        for count, item in enumerate(iter(self._iterator)):
            if count == items:
                return
            yield item

    def filter(self, filters):
        """
        Filter a _DictSet_ returning only the items that match the predicate.

        Parameters:
            predicate: callable
                A function that takes a record as a parameter and should return
                False for items to be filtered
        """
        # Where clause filtering
        if isinstance(filters, str):

            def inner_filter_where(dictset):
                for record in dictset:
                    if q.evaluate(record):
                        yield record

            q = Expression(filters)
            return DictSet(
                inner_filter_where(iter(self._iterator)),
                storage_class=self.storage_class,
            )

        # DNF filtering
        if isinstance(filters, (tuple, list)):
            filter_set = DnfFilters(filters)
            return DictSet(
                DnfFilters.filter_dictset(filter_set, iter(self._iterator)),
                storage_class=self.storage_class,
            )

        # function filtering
        if hasattr(filters, "__call__"):

            def inner_filter_callable(func, dictset):
                for item in dictset:
                    if func(item):
                        yield item

            return DictSet(
                inner_filter_callable(filters, iter(self._iterator)),
                storage_class=self.storage_class,
            )

    def cursor(self):
        """
        If the DictSet supports cursors, return the cursor.
        """
        if hasattr(self._iterator, "cursor"):
            return self._iterator.cursor
        return None

    def select(self, columns):
        """
        Selects columns from a _DictSet_. If the column doesn't exist it is populated
        with `None`.
        """
        if not isinstance(columns, (list, set, tuple)):
            columns = set([columns])

        def inner_select(it):
            for record in it:
                yield {k: record.get(k, None) for k in columns}

        return DictSet(
            inner_select(iter(self._iterator)), storage_class=self.storage_class
        )

    def sort_and_take(self, column, take: int = 5000, descending: bool = False):

        if self.storage_class == STORAGE_CLASS.MEMORY:
            yield from sorted(
                self._iterator, key=itemgetter(column), reverse=descending
            )[:take]

        else:
            # In a low-memory environment we probably can't store all of the records
            # into memory, but if we're only interested in, say the top 10, then we
            # only need to store about that many in memory at any one time. This
            # implementation stores double and one records in memory as it collects
            # and sorts them.
            double_cache = max(take * 2, 1) + 1
            cache = []
            for record in iter(self._iterator):
                cache.append(record)
                if len(cache) > double_cache:
                    cache.sort(key=itemgetter(column), reverse=descending)
                    del cache[take:]
            cache.sort(key=itemgetter(column), reverse=descending)
            yield from cache[:take]

    def __getitem__(self, columns):
        """
        Select the columns from the _DictSet_, alias for .select
        """
        return self.select(columns)

    def __hash__(self, seed: int = 703115) -> int:
        """
        Creates a consistent hash of the _DictSet_ regardless of the order of
        the items in the _DictSet_.
        """

        def sip(val):
            return siphash("*", val)

        # The seed is the mission duration of the Apollo 11 mission.
        #   703115 = 8 days, 3 hours, 18 minutes, 35 seconds
        ordered = map(lambda record: dict(sorted(record.items())), iter(self._iterator))
        serialized = map(orjson.dumps, ordered)
        hashed = map(sip, serialized)
        return reduce(lambda x, y: x ^ y, hashed, seed)

    def __repr__(self):  # pragma: no cover
        if is_running_from_ipython():
            from IPython.display import HTML, display  # type:ignore

            html = html_table(iter(self._iterator), 10)
            display(HTML(html))
            return ""  # __repr__ must return something
        else:
            return ascii_table(iter(self._iterator), 10)
