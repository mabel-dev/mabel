import io
import struct
from operator import itemgetter
from typing import Iterable, Any
from functools import lru_cache
from cityhash import CityHash32


MAX_INDEX = 4294967295  # 2^32 - 1
STRUCT_DEF = "I I I"  # 4 byte unsigned int, 4 byte unsigned int, 4 byte unsigned int
RECORD_SIZE = struct.calcsize(STRUCT_DEF)  # this should be 12

"""
There are overlapping terms because we're traversing a dataset so we can traverse a
dataset. 

Terminology:
    Entry     : a record in the Index
    Location  : the position of the entry in the Index
    Position  : the position of the row in the target file
    Row       : a record in the target file
"""

# hashing can be slow, avoid if we can just convert to a number
CONVERTERS = {
    "int": lambda x: x,
    "date": lambda x: (x.year * 1000) + (x.month * 10) + x.day,
    "datetime": lambda x: int.from_bytes(struct.pack("d", x.timestamp()), "big"),
    "float": lambda x: int.from_bytes(struct.pack("d", x), "big"),
}


def fallback_converter(val):
    return CityHash32(val)


def value_to_int(val: Any):
    val_type = type(val).__name__
    converter = fallback_converter
    if val_type in CONVERTERS:
        converter = CONVERTERS[val_type]
    return converter(val)


class IndexEntry(object):
    """
    Python friendly representation of index entries.
    Includes binary translations for reading and writing to the index.
    """

    __slots__ = ("value", "location", "count")

    def to_bin(self) -> bytes:
        """
        Convert a model to _bytes_
        """
        return struct.pack(STRUCT_DEF, self.value, self.location, self.count)

    @staticmethod
    def from_bin(buffer):
        """
        Convert _bytes_ to a model
        """
        value, location, count = struct.unpack(STRUCT_DEF, buffer)
        return IndexEntry(value=value, location=location, count=count)

    def __init__(self, value, location, count):
        self.value = value
        self.location = location
        self.count = count


class Index:
    def __init__(self, index: io.BytesIO):
        """
        A data index which speeds up reading data files.

        The file format is fixed-length binary, the search algorithm is a
        classic binary search.
        """
        if isinstance(index, io.BytesIO):
            self._index = index
            # go to the end of the stream
            index.seek(0, 2)
            # divide the size of the stream by the record size to get the
            # number of entries in the index
            self.size = index.tell() // RECORD_SIZE

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
            io.BytesIO
        """
        # We do this in two-steps
        # 1) Build an intermediate form of the index as a list of entries
        # 2) Conver that intermediate form into a binary index
        builder = IndexBuilder(column_name)
        for position, row in enumerate(dictset):
            builder.add(position, row)
        return builder.build()

    @lru_cache(8)
    def _get_entry(self, location: int):
        """
        get a specific entry from the index
        """
        try:
            self._index.seek(RECORD_SIZE * location)
            return IndexEntry.from_bin(self._index.read(RECORD_SIZE))
        except Exception:
            return None

    def _locate_record(self, value):
        """
        Use a binary search algorithm to search the index
        """
        left, right = 0, (self.size - 1)
        while left <= right:
            middle = (left + right) >> 1
            entry = self._get_entry(middle)
            if entry.value == value:
                return middle, entry
            elif entry.value > value:
                right = middle - 1
            else:
                left = middle + 1
        return -1, None

    def _inner_search(self, search_term) -> Iterable:
        # hash the value and make fit in a four byte unsinged int
        value = value_to_int(search_term) % MAX_INDEX

        # search for an instance of the value in the index
        location, found_entry = self._locate_record(value)

        # we didn't find the entry
        if not found_entry:
            return []

        # the found_entry is the fastest record to be found, this could
        # be the first, last or middle of the set. The count field tells
        # us how many rows to go back, but not how many forward
        start_location = location - found_entry.count + 1
        end_location = location + 1
        while end_location < self.size and self._get_entry(end_location).value == value:
            end_location += 1

        # extract the row numbers in the target dataset
        return [
            self._get_entry(loc).location
            for loc in range(start_location, end_location, 1)
        ]

    def search(self, search_term) -> Iterable:
        """
        Search the index for a value. Returns a list of row numbers, if the value is
        not found, the list is empty.
        """
        if not isinstance(search_term, (list, set, tuple)):
            search_term = [search_term]
        result: list = []
        for term in search_term:
            result[0:0] = self._inner_search(term)
        return set(result)


class IndexBuilder:

    slots = ("column_name", "temporary_index")

    def __init__(self, column_name: str):
        self.column_name: str = column_name
        self.temporary_index: Iterable[dict] = []

    def add(self, position, record):
        if record.get(self.column_name):
            # index lists of items separately
            values = record[self.column_name]
            if not isinstance(values, list):
                values = [values]
            for value in values:
                entry = {
                    "value": value_to_int(value) % MAX_INDEX,
                    "position": position,
                }
                self.temporary_index.append(entry)

    def build(self) -> Index:
        previous_value = None
        index = bytes()
        count: int = 0
        self.temporary_index = sorted(self.temporary_index, key=itemgetter("value"))
        for row in self.temporary_index:
            if row["value"] == previous_value:
                count += 1
            else:
                count = 1
            index += IndexEntry(
                value=row["value"], location=row["position"], count=count
            ).to_bin()
            previous_value = row["value"]
        return Index(io.BytesIO(index))
