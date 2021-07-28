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

import enum
import os
import time
import orjson
import statistics

from tempfile import TemporaryDirectory
from functools import reduce

from typing import Iterable, Union
from juon.dictset.display import html_table, ascii_table

# from ....logging import get_logger
from ...readers import STORAGE_CLASS
from ....errors import MissingDependencyError, InvalidArgument


from operator import itemgetter
from enum import Enum

from .disk_iterator import DiskIterator


MAXIMUM_RECORDS_IN_PARTITION = 65535  # 2^16 -1


class DictSet(object):
    def __init__(self, iterator: Iterable, storage_class=STORAGE_CLASS.NO_PERSISTANCE):
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

    def __del__(self):
        try:
            if self._temporary_folder:
                self._temporary_folder.cleanup()
        except:
            pass

    def persist(self, storage_class=STORAGE_CLASS.MEMORY):
        if storage_class == STORAGE_CLASS.NO_PERSISTANCE:
            raise InvalidArgument("Persist cannot persist to 'NO_PERISISTANCE'")
        if self.storage_class == storage_class:
            return None
        if storage_class == STORAGE_CLASS.MEMORY:
            self._iterator == list(self._iterator)
            if self._temporary_folder:
                self._temporary_folder.cleanup()
                self._temporary_folder = None
        if storage_class == STORAGE_CLASS.DISK:
            self._persist_to_disk()
        self.storage_class = storage_class

    def sample(self, fraction: float = 0.5):
        """
        Select a random stample of
        """
        selector = int(1 / fraction)
        for row in self._iterator:
            random_value = int.from_bytes(os.urandom(2), "big")
            if random_value % selector == 0:
                yield row

    def collect(self, key: str = None) -> Union[list, map]:
        """
        Convert a _DictSet_ to a list, optionally, but probably usually, just extract
        a specific column.
        """
        if not key:
            return list(self._iterator)
        return map(itemgetter(key), self._iterator)

    def keys(self, number_of_rows: int = 10):
        """
        Get all of the keys from the _DictSet_. This iterates through the entire
        _DictSet_.
        """
        if number_of_rows > 0:
            rows = self.itake(number_of_rows)
            return reduce(lambda x, y: [a for a in y.keys() if a not in x], rows, [])
        return reduce(
            lambda x, y: [a for a in y.keys() if a not in x], self._iterator, []
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

    def count(self):
        """
        Count the number of items in the _DictSet_.
        """
        # we use `reduce` so we don't need to load all of the items into a list
        # in order to count them.
        return reduce(lambda x, y: x + 1, self._iterator, 0)

    def distinct(self):
        """
        Remove duplicates from a _DictSet_.
        """
        return reduce(lambda x, y: x + [y] if not y in x else x, self._iterator, [])

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

    def take(self, items: int):
        """
        Return the first _items_ number of items from the _DictSet_. This loads
        these items into memory. If returning a large number of items, use itake.
        """
        result = []
        for count, item in enumerate(self._iterator):
            if count == items:
                return result
            result.append(item)
        return result

    def itake(self, items: int):
        """
        Return the first _items_ number of items from the _DictSet_. This returns
        a generator.
        """
        result = []
        for count, item in enumerate(self._iterator):
            if count == items:
                return
            yield item

    def filter(self, predicate):
        """
        Filter a _DictSet_ returning only the items that match the predicate.
        """
        return [item for item in filter(predicate, self._iterator)]

    def __hash__(self, seed: int = 703115) -> int:
        """
        Creates a consistent hash of the _DictSet_.

        703115 = 8 days, 3 hours, 18 minutes, 35 seconds
        """
        import cityhash

        return reduce(
            lambda x, y: x ^ cityhash.CityHash32(orjson.dumps(y)), self._iterator, seed
        )
