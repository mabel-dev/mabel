import io
import mmh3  # type:ignore
import struct
from operator import itemgetter
from pydantic import BaseModel    # type:ignore
from functools import lru_cache

UNSET = 65535  # 2^16 - 1
MAX_INDEX = 4294967295  # 2^32 - 1
STRUCT_DEF = "I H H"  # 4 byte unsigned int, 2 byte unsigned int, 2 byte unsigned int
RECORD_SIZE = struct.calcsize(STRUCT_DEF)  # this should be 8

"""
There are overlapping terms because we're traversing a dataset so we can
traverse a dataset. 

Terminology:
    Entry     : a record in the Index
    Location  : the position of the entry in the Index
    Position  : the position of the row in the target file
    Row       : a record in the target file
"""

def safe_field_name(field_name):
    """strip all the non-alphanums from a field name"""
    import re, string;
    pattern = re.compile('[\W_]+')
    return pattern.sub('', field_name)


class IndexEntry(BaseModel):
    """
    Python friendly representation of index entries.

    Includes binary translations for reading and writing to the index.
    """
    value: int
    location: int
    count: int

    def to_bin(self) -> bytes:
        return struct.pack(
                STRUCT_DEF,
                self.value,
                self.location,
                self.count)

    @staticmethod
    def from_bin(buffer):
        value, location, count = struct.unpack(STRUCT_DEF, buffer)
        return IndexEntry(
                value=value,
                location=location,
                count=count)


class Index():

    def __init__(
            self,
            index: io.BytesIO):
        if isinstance(index, io.BytesIO):
            self._index = index
            # go to the end of the stream
            index.seek(0,2)
            # divide the size of the stream by the record size to get the
            # number of entries in the index
            self.size = index.tell() // RECORD_SIZE

    @staticmethod
    def build_index(dictset, column):
        temp = []
        index = bytes()
        for position, row in enumerate(dictset):
            if row.get(column):
                entry = {
                        "value": mmh3.hash(row[column]) % MAX_INDEX,
                        "position": position
                }
            temp.append(entry)
        previous_value = None
        for i, row in enumerate(sorted(temp, key=itemgetter("value"))):
            #print(i, row)
            if row['value'] == previous_value:
                count += 1
            else:
                count = 1
            index += IndexEntry(
                    value=row['value'],
                    location=row['position'],
                    count=count).to_bin()
            previous_value = row['value']
        return Index(io.BytesIO(index))

    @lru_cache(8)
    def _get_entry(self, location):
        if location >= self.size:
            return IndexEntry(
                value=-1,
                location=-1,
                count=-1
            )
        self._index.seek(RECORD_SIZE * location)
        return IndexEntry.from_bin(self._index.read(RECORD_SIZE))

    def _locate_record(self, value):
        """
        Use a binary search algorithm to search the index
        """
        left, right = 0, self.size
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

    def search(self, search_term):
        # hash the value and make fit in a four byte unsinged int
        value = mmh3.hash(search_term) % MAX_INDEX

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
        while self._get_entry(end_location).value == value:
            end_location += 1

        # extract the row numbers in the target dataset
        return {self._get_entry(loc).location for loc in range(start_location, end_location, 1)}


    def __repr__(self):
        return str(len(self._index))
