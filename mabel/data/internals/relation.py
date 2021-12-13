"""
Implement a Relation, this analogous to a database table. It is called a Relation in
line with terminology for Relational Algreba and technically makes Mabel a Relational
Database although probably not charactistically.

Internally the data for the Relation is stored as a list of Tuples and a dictionary
records the schema and other key information about the Relation. Tuples are faster
than Dicts for accessing data.

Some naive benchmarking with 135k records (with 269 columns):
- dataset is 30% smaller
- Dedupe 90% faster

We don't use numpy arrays for Relations, which documentation would have you believe
is orders of magnititude faster again because we are storing heterogenous data which
at the moment I can't think of a good way implement without hugh overheads to deal
with this limitation.


- self.data is an array of tuples
- self.header is a dictionary
    {
        "attribute": {
            "type": type (domain)
            "min": smallest value in this attribute
            "max": largest value in this attribute
            "count": count of non-null values in attribute
            "unique": number of unique values in this attribute
        }
    }

    Only "type" can be assumed to be present, especially after a select operation
"""
import sys
import os

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import datetime
from typing import Iterable, Tuple
from mabel.data.internals.dictset import DictSet
from mabel.data.internals.algorithms.hyper_log_log import HyperLogLog


class Relation:

    __slots__ = ("header", "data", "name")

    def __init__(self, data: Iterable[Tuple] = [], header: dict = {}, name: str = None):
        self.data = data
        self.header = header
        self.name = name

    def apply_selection(self, predicate):
        """
        Apply a Selection operation to a Relation, this filters the data in the
        Relation to just the entries which match the predicate.

        Parameters:
            predicate (callable):
                A function which can be applied to a tuple to determine if it
                should be returned in the target Relation.

        Returns:

        """
        # selection invalidates what we thought we knew about counts etc
        new_header = {k: {"type": v.get("type")} for k, v in self.header.items()}
        return Relation(filter(predicate, self.data), new_header)

    def apply_projection(self, attributes):
        if not isinstance(attributes, (list, tuple)):
            attributes = [attributes]
        attribute_indices = []
        new_header = {k: v for k, v in self.header.items() if k in attributes}
        for index, attribute in enumerate(self.header.keys()):
            if attribute in attributes:
                attribute_indices.append(index - 1)

        def _inner_projection():
            for tup in self.data:
                yield tuple([tup[indice] for indice in attribute_indices])

        return Relation(_inner_projection(), new_header)

    def materialize(self):
        if not isinstance(self.data, list):
            self.data = list(self.data)
        return self.data

    def count(self):
        self.materialize()
        return len(self.data)

    def distinct(self):
        """
        Return a new Relation with only unique values
        """
        hash_list = {}

        def do_dedupe(data):
            for item in data:
                hashed_item = hash(item)
                if hashed_item not in hash_list:
                    yield item
                    hash_list[hashed_item] = True

        return Relation(do_dedupe(self.data), self.header)

    def from_dictset(self, dictset: DictSet):
        """
        Load a Relation from a DictSet.

        In this case, the entire Relation is loaded into memory.
        """

        self.header = {k: {"type": v} for k, v in dictset.types().items()}
        self.data = [
            tuple([self._coerce(row.get(k)) for k in self.header.keys()])
            for row in dictset.icollect_list()
        ]

    def to_dictset(self):
        """
        Convert a Relation to a DictSet, this will fill every column so missing entries
        will be populated with Nones.
        """

        def _inner_to_dictset():
            yield from [
                {k: row[i] for i, (k, v) in enumerate(self.header.items())}
                for row in self.data
            ]

        return DictSet(_inner_to_dictset())

    def attributes(self):
        return [k for k in self.header.keys()]

    def __str__(self):
        return f"{self.name or 'Relation'} ({', '.join([k + ':' + v.get('type') for k,v in self.header.items()])})"

    def __len__(self):
        """
        Alias for .count
        """
        return self.count()

    def _coerce(self, var):
        """
        Relations only support a subset of types, if we know how to translate a type
        into a supported type, do it
        """
        t = type(var)
        if t in (int, float, tuple, bool, str, datetime.datetime, None):
            return var
        if t in (list, set):
            return tuple(var)
        if t in (datetime.date,):
            return datetime.datetime(t.year, t.month, t.day)


if __name__ == "__main__":
    from mabel.data import STORAGE_CLASS, Reader
    from mabel.adapters.disk import DiskReader

    ds = Reader(
        inner_reader=DiskReader, dataset="tests/data/half", raw_path=True
    )  # , persistence=STORAGE_CLASS.MEMORY)
    ds = DictSet(ds.sample(0.1), storage_class=STORAGE_CLASS.MEMORY)

    import sys
    from types import ModuleType, FunctionType
    from gc import get_referents

    # Custom objects know their class.
    # Function objects seem to know way too much, including modules.
    # Exclude modules as well.
    BLACKLIST = type, ModuleType, FunctionType

    def getsize(obj):
        """sum size of object & members."""
        if isinstance(obj, BLACKLIST):
            raise TypeError(
                "getsize() does not take argument of type: " + str(type(obj))
            )
        seen_ids = set()
        size = 0
        objects = [obj]
        while objects:
            need_referents = []
            for obj in objects:
                if not isinstance(obj, BLACKLIST) and id(obj) not in seen_ids:
                    seen_ids.add(id(obj))
                    size += sys.getsizeof(obj)
                    need_referents.append(obj)
            objects = get_referents(*need_referents)
        return size

    rel = Relation()
    rel.from_dictset(ds)

    # print(getsize(rel))
    # print(getsize(ds))

    from mabel.utils.timer import Timer
    from mabel.data.internals.zone_map_writer import ZoneMapWriter

    with Timer("rel"):
        for i in range(10):
            # r = rel.apply_selection(lambda x: x[0] == "270").count()
            r = rel.distinct().count()
    print(r)

    # with Timer("ds"):
    #    for i in range(10):
    #        #        #d = ds.select(lambda x: x["number"] == "270").count()
    #        r = ds.distinct().count()
    # print(r)

    # print(rel.apply_projection("description").to_dictset())

    print(rel.attributes(), len(rel.attributes()))

    with Timer("without mapper"):
        for i in [a for a in ds._iterator]:
            pass

    with Timer("with mapper"):
        zm = ZoneMapWriter()
        for i in [a for a in ds._iterator]:
            zm.add(i)

    from pprint import pprint

    pprint(list(zm.profile()))
