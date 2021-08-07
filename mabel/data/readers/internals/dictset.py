"""
DICT(IONARY) (DATA)SET

A group of functions to assist with handling lists of dictionaries.

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
import time
import orjson
import cityhash
import statistics


from tempfile import TemporaryDirectory
from functools import reduce

from typing import Iterable, Union
from juon.dictset.display import html_table, ascii_table

# from ....logging import get_logger
from ...readers import STORAGE_CLASS
from ....errors import MissingDependencyError, InvalidArgument


from operator import itemgetter

from .disk_iterator import DiskIterator
from .expression import Expression
from .filters import Filters


MAXIMUM_RECORDS_IN_PARTITION = 65535  # 2^16 -1


class DictSet(object):
    def __init__(
        self,
        iterator: Iterable,
        *,
        storage_class=STORAGE_CLASS.NO_PERSISTANCE,
        number_of_partitions: int = 4,
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
                but can handle much larger data sets.
            number_of_partitions: integer (optional)

        """
        self.storage_class = storage_class
        self._iterator = iterator
        self._temporary_folder = None

        # if we're persisting to memory, load into a list
        if storage_class == STORAGE_CLASS.MEMORY:
            self._iterator = list(iterator)

        # if we're persisting to disk, save it
        if storage_class == STORAGE_CLASS.DISK:
            self._persist_to_disk()

    def _persist_to_disk(self):
        # save the data to a temporary folder
        file = None
        self._temporary_folder = TemporaryDirectory("dictset")
        os.makedirs(self._temporary_folder.name, exist_ok=True)
        for index, row in enumerate(self._iterator):
            if index % MAXIMUM_RECORDS_IN_PARTITION == 0:
                if file:
                    file.close()
                file = open(
                    f"{self._temporary_folder.name}/{time.time_ns()}.jsonl", "wb"
                )
            file.write(orjson.dumps(row) + b"\n")
        if file:
            file.close()
        self._iterator = DiskIterator(self._temporary_folder.name)

    def __iter__(self):
        return iter(self._iterator)

    def __next__(self):
        return next(self._iterator)

    def __del__(self):
        try:
            if self._temporary_folder:
                self._temporary_folder.cleanup()
        except:  # nosec
            pass

    def persist(self, storage_class=STORAGE_CLASS.MEMORY):
        if storage_class == STORAGE_CLASS.NO_PERSISTANCE:
            raise InvalidArgument("Persist cannot persist to 'NO_PERISISTANCE'")
        if self.storage_class == storage_class:
            return False
        if storage_class == STORAGE_CLASS.MEMORY:
            self._iterator == list(self._iterator)
            if self._temporary_folder:
                self._temporary_folder.cleanup()
                self._temporary_folder = None
        if storage_class == STORAGE_CLASS.DISK:
            self._persist_to_disk()
        self.storage_class = storage_class
        return True

    def sample(self, fraction: float = 0.5):
        """
        Select a random stample of
        """

        def inner_sampler(dictset):
            selector = int(1 / fraction)
            for row in dictset:
                random_value = int.from_bytes(os.urandom(2), "big")
                if random_value % selector == 0:
                    yield row

        return DictSet(inner_sampler, self.storage_class)

    def collect(self, key: str = None) -> Union[list, map]:
        """
        Convert a _DictSet_ to a list, optionally, but probably usually, just extract
        a specific column.
        """
        if not key:
            return list(self._iterator)
        return list(map(itemgetter(key), self._iterator))

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
            lambda x, y: x + [a for a in y.keys() if a not in x], self._iterator, []
        )

    def aggregate(self, function: callable, key: str):
        # perform a function on all of the items in the
        # set, e.g. fold([1,2,3],add) == 6
        return reduce(function, self.collect(key))

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

    def item(self, index):
        return self._iterator[index]

    def count(self):
        """
        Count the number of items in the _DictSet_.
        """
        # we use `reduce` so we don't need to load all of the items into a list
        # in order to count them.
        if self.storage_class in (STORAGE_CLASS.MEMORY, STORAGE_CLASS.DISK):
            return reduce(lambda x, y: x + 1, self._iterator, 0)
        else:
            # we can't count the items in an non persisted DictSet
            return -1

    def distinct(self):
        """
        Remove duplicates from a _DictSet_. This creates a list of the items
        already added to the result, so is not suitable for huge _DictSets_.
        """
        hash_list = {}

        def do_dedupe(data):
            for item in data:
                hashed_item = hash(orjson.dumps(item))
                if hashed_item not in hash_list:
                    yield item
                else:
                    hash_list[hashed_item] = True

        return DictSet(do_dedupe(self._iterator), self.storage_class)

    def igroupby(self, column):
        """ """
        from ....index.index import IndexBuilder

        builder = IndexBuilder(column)
        for i, r in enumerate(self._iterator):
            builder.add(i, r)
        index = builder.build()

        position = 0
        entry = index._get_entry(position)
        item_locations = []
        while entry:
            # get the uncompressed value
            stored_value = self._iterator[entry.location].pop().get(column)
            item_locations.append(entry.location)

            # get the positions of the values in the index
            end_location = position + 1
            this_entry = index._get_entry(end_location)
            while end_location < index.size and this_entry.value == entry.value:
                item_locations.append(this_entry.location)
                end_location += 1
                this_entry = index._get_entry(end_location)

            yield stored_value, DictSet(
                self._iterator[item_locations], storage_class=self.storage_class
            )
            position = end_location
            entry = index._get_entry(position)
            item_locations = []

    def to_ascii_table(self, limit: int = 5):
        """
        Return the top `limit` rows from a _DictSet_ as an ASCII table.

        Returns:
            Table encoded in a string
        """
        return ascii_table(self._iterator, limit)

    def to_html_table(self, limit: int = 5):
        """
        Return the top `limit` rows from a _DictSet_ as a HTML table.

        Returns:
            HTML Table encoded in a string
        """
        return html_table(self._iterator, limit)

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
        return pandas.DataFrame(self)

    def first(self):
        return next(iter(self._iterator), None)

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
        for count, item in enumerate(self._iterator):
            if count == items:
                return
            yield item

    def filter(self, predicate):
        """
        Filter a _DictSet_ returning only the items that match the predicate.

        Parameters:
            predicate: callable
                A function that takes a record as a parameter and should return
                False for items to be filtered
        """

        def inner_filter(func, dictset):
            for item in dictset:
                if func(item):
                    yield item

        return DictSet(inner_filter(predicate, self._iterator), self.storage_class)

    def dnf_filter(self, dnf_filters):
        """
        Filter a _DictSet_ returning only the items that match the predicates.

        Parameters:
            dnf_filters: tuple or list
                DNF constructed predicates
        """
        filter_set = Filters(dnf_filters)
        return DictSet(
            Filters.filter_dictset(filter_set, self._iterator), self.storage_class
        )

    def query(self, expression):
        """
        Query a _DictSet_ returning only the items that match the expression.

        Parameters:
            expression: string
                Query expression (e.g. _name == 'mabel'_)
        """
        q = Expression(expression)

        def _inner(dictset):
            for record in dictset:
                if q.evaluate(record):
                    yield record

        return DictSet(_inner(self._iterator), storage_class=self.storage_class)

    def cursor(self):
        if hasattr(self._iterator, "cursor"):
            return self._iterator.cursor
        return None

    def __getitem__(self, columns):
        """
        Selects columns from a _DictSet_. If the column doesn't exist it is populated
        with `None`.
        """
        if not isinstance(columns, (list, set, tuple)):
            columns = set([columns])

        def inner_select(it):
            for record in it:
                yield {k: record.get(k, None) for k in columns}

        return DictSet(inner_select(self._iterator), self.storage_class)

    def __hash__(self, seed: int = 703115) -> int:
        """
        Creates a consistent hash of the _DictSet_ regardless of the order of
        the items in the _DictSet_.

        703115 = 8 days, 3 hours, 18 minutes, 35 seconds
        """
        serialized = map(orjson.dumps, self._iterator)
        hashed = map(cityhash.CityHash32, serialized)
        return reduce(lambda x, y: x ^ y, hashed, seed)

    def _parallelize(self, func):
        pass
