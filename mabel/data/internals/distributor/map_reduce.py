from abc import ABC
import abc
from mabel import data
from typing import Iterable, Tuple

from itertools import groupby
from operator import itemgetter


class MapReduce(ABC):
    def __init__(self, **kwargs):
        pass

    @abc.abstractclassmethod
    def map(self, partition) -> Iterable[Tuple]:
        """
        Map takes a data set, performs a task against the data and returns an
        iterable of K/V pairs (as a list of tuples)
        """
        pass

    def combine(self, pairs):
        return pairs

    def shuffle(self, pairs):
        yield from groupby(sorted(pairs, key=itemgetter(1)), key=itemgetter(1))

    def reduce(self, pairs: Iterable[Tuple]):
        return [v for k, v in pairs]

    def emit(self, reduced):
        return reduced


class MRGroupBy(MapReduce):
    def __init__(self, **kwargs):
        self._column = kwargs.get("column")

    def map(self, partition):
        for index, row in enumerate(partition):
            yield (index, hash(row.get(self._column, "")))

    def combine(self, pairs):
        return pairs

    def reduce(self, groups):
        for group in groups:
            print("GROUP>>", group)

    #        return [v for k, v in groups]

    def emit(self, reduced):
        return (self._column, reduced)


dataset = [
    {"a": "apple", "b": 2},
    {"a": "apple", "b": 1},
    {"a": "pear", "b": 5},
    {"a": "banana", "b": 4},
]

mrg = MRGroupBy(column="a")
mapped = mrg.map(dataset)
shuffled = list(mrg.shuffle(mapped))
print("shuffled", shuffled)
for group, shuff in shuffled:
    print(group)
    print(list(shuff))
    # reduced = mrg.reduce()
    # print(reduced)
# print(mrg.emit(reduced))

gs = groupby(dataset, itemgetter("a"))
for k, g in gs:
    print(k, list(g))
