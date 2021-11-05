"""
Index is a json KV store:

{ 
    "hash(value)": [list of locations]
}
"""
from typing import Iterable
import orjson
from siphashc import siphash

MAX_INDEX = 4294967295  # 2^32 - 1
SEED = "eschatologically"  # needs to be 16 characters long

"""
There are overlapping terms because we're traversing a dataset so we can traverse a
dataset. 

Terminology:
    Entry     : a record in the Index
    Position  : the position of the entry in the Index
    Location  : the position of the row in the target file
    Row       : a record in the target file
"""


class Index:
    def __init__(self, index: bytes):
        if hasattr(index, "read"):
            index = index.read()  # type:ignore

        try:
            import simdjson

            self._index = simdjson.Parser().parse(index)
        except ImportError:
            self._index = orjson.loads(index)

    @staticmethod
    def build_index(dictset: Iterable[dict], column_name: str):
        """
        Build an index from a dictset.

        Parameters:
            dictset: iterable of dictionaries
                The dictset to index
            column_name: string
                The name of the index which will be indexed

        Returns:
            bytes
        """
        # We do this in two-steps
        # 1) Build an intermediate form of the index as a list of entries
        # 2) Conver that intermediate form into a binary index
        builder = IndexBuilder(column_name)
        for position, row in enumerate(dictset):
            builder.add(position, row)
        return builder.build()

    def search(self, search_term) -> Iterable:
        """
        Search the index for a value. Returns a list of row numbers, if the value is
        not found, the list is empty.
        """
        if not isinstance(search_term, (list, set, tuple)):
            search_term = [search_term]
        result: list = []
        for term in search_term:
            key = format(siphash(SEED, f"{term}") % MAX_INDEX, "x")
            if key in self._index:  # type:ignore
                result[0:0] = self._index[key]  # type:ignore
        return result

    def dump(self, file):
        with open(file, "wb") as f:
            f.write(self.bytes())

    def bytes(self):
        if hasattr(self._index, "mini"):
            return self._index.mini
        import orjson

        return orjson.dumps(self._index)


class IndexBuilder:

    slots = ("column_name", "temporary_index")

    def __init__(self, column_name: str):
        self.column_name: str = column_name
        self.temporary_index: Iterable[dict] = []

    def add(self, position, record):
        ret_val = []
        if record.get(self.column_name):
            # index lists of items separately
            values = record[self.column_name]
            if not isinstance(values, list):
                values = [values]
            for value in values:
                entry = (format(siphash(SEED, f"{value}") % MAX_INDEX, "x"), position)
                ret_val.append(entry)
        self.temporary_index += ret_val
        return ret_val

    def build(self) -> Index:
        temp_index = {}
        for val, pos in self.temporary_index:
            if not val in temp_index:
                temp_index[val] = [pos]
            else:
                poses = temp_index[val]
                poses.append(pos)
                temp_index[val] = poses
        import orjson

        return Index(orjson.dumps(temp_index))
