"""
StorageClassDisk is a helper class for persisting DictSets locally, it is the backend
for the BINARY_DISK variation of the STORAGE CLASSES.

This stores DICTSETs in a binary format - which should be smaller and faster - but only
supports a subset of field types.
"""
from mabel.errors.data_not_found_error import DataNotFoundError
import os
import sys
import mmap
import atexit
import struct
import datetime
from tempfile import NamedTemporaryFile
from typing import Iterable, Any, Iterator
from ...utils.paths import silent_remove


from ctypes import create_string_buffer

STRING_LENGTH = 32


def date_dumper(var):
    date = parse_iso(var)
    if date:
        return int(date.timestamp()).to_bytes(4, "big", signed=False)
    return b"\x00" * 4


def date_loader(var):
    ts = int.from_bytes(var, "big", signed=False)
    return datetime.datetime.fromtimestamp(ts)


def load_float(var):
    var = struct.unpack("<d", var)[0]
    if var == sys.float_info.min:
        return None
    return var


def dump_float(var):
    if var is None:
        var = sys.float_info.min
    return struct.pack("<d", var).ljust(8, b"\x00")


def dump_str(var):
    # return create_string_buffer(var.encode()[:STRING_LENGTH], STRING_LENGTH).raw
    return str.encode(var.ljust(STRING_LENGTH, "\x00"))[:STRING_LENGTH]


def dump_int(var):
    if var:
        return var.to_bytes(8, "big")
    return b"\x00" * 8


def load_int(var):
    return int.from_bytes(var, "big")


BUFFER_SIZE = 64 * 1024 * 1024

TYPE_STORAGE = {
    "int": (8, dump_int, load_int),
    "float": (8, dump_float, load_float),
    "str": (STRING_LENGTH, dump_str, lambda x: x.split(b"\x00", 1)[0].decode()),
    "datetime": (4, date_dumper, date_loader),
    "bool": (1, lambda x: b"\x01" if x else b"\x00", lambda x: x == b"\x01"),
    "spacer": (1, lambda x: b"\x00", lambda x: None),
}


def parse_iso(value):
    DATE_SEPARATORS = {"-", ":"}
    # date validation at speed is hard, dateutil is great but really slow, this is fast
    # but error-prone. It assumes it is a date or it really nothing like a date.
    # Making that assumption - and accepting the consequences - we can convert upto
    # three times faster than dateutil.
    try:
        if isinstance(value, (datetime.datetime)):
            return value
        if isinstance(value, str) and len(value) >= 10:
            if not value[4] in DATE_SEPARATORS or not value[7] in DATE_SEPARATORS:
                return None
            if len(value) == 10:
                # YYYY-MM-DD
                return datetime.datetime(
                    *map(int, [value[:4], value[5:7], value[8:10]])
                )
            if len(value) >= 16:
                if not value[10] in {"T", " "} or not value[13] in DATE_SEPARATORS:
                    return False
                # YYYY-MM-DDTHH:MM
                return datetime.datetime(
                    *map(  # type:ignore
                        int,
                        [
                            value[:4],
                            value[5:7],
                            value[8:10],
                            value[11:13],
                            value[14:16],
                        ],
                    )
                )
        return None
    except (ValueError, TypeError):
        return None


class StorageClassBinaryDisk(object):
    """
    This provides the reader for the BINARY_DISK variation of STORAGE.
    """

    ## roughly 25% of the time is serializing
    def serialize(self, record: dict) -> bytes:
        buffer = bytes()
        for k, v in self.schema_dict.items():
            size, dumper, loader = TYPE_STORAGE[v]
            if k in record:
                buffer += dumper(record[k])
            else:
                buffer += b"\x00" * size
        return buffer

    # very little execution time
    def deserialize(self, record: bytes) -> dict:
        result = {}
        cursor = 0
        for k, v in self.schema_dict.items():
            size, dumper, loader = TYPE_STORAGE[v]
            result[k] = loader(record[cursor : cursor + size])
            cursor += size
        return result

    @staticmethod
    def determine_schema(record: dict) -> dict:
        schema = {}
        for k, v in record.items():
            value_type = type(v).__name__
            if value_type == "str" and parse_iso(v):
                value_type = "datetime"
            elif value_type not in TYPE_STORAGE:
                value_type = "spacer"
            schema[k] = value_type
        return schema

    def __init__(self, iterator):
        try:
            # if next fails, we're probably reading an empty set
            record = next(iterator, {})
            self.schema_dict = self.determine_schema(record)
            self.schema_size = sum(
                [TYPE_STORAGE[v][0] for k, v in self.schema_dict.items()]
            )
        except:
            raise DataNotFoundError("Unable to retrieve records")

        self.inner_reader = None
        self.length = -1

        self.file = NamedTemporaryFile(prefix="mabel-dictset").name
        atexit.register(silent_remove, filename=self.file)

        with open(self.file, "wb") as f:
            f.write(self.serialize(record))
            for self.length, row in enumerate(iterator, start=1):
                f.write(self.serialize(row))

        self.length += 1
        self.iterator = None

    def _read_file(self):
        with open(self.file, mode="rb") as file_obj:
            with mmap.mmap(
                file_obj.fileno(), length=0, access=mmap.ACCESS_READ
            ) as mmap_obj:
                cursor = 0
                file_size = len(mmap_obj)
                record_size = self.schema_size
                while cursor < file_size:
                    yield mmap_obj[cursor : cursor + record_size]
                    cursor += record_size

    def _inner_reader(self, *locations):
        deserialize = self.deserialize
        if locations:
            max_location = max(locations)
            min_location = min(locations)

            reader = self._read_file()

            for i in range(min_location):
                next(reader)

            for i, line in enumerate(reader, min_location):
                if i in locations:
                    yield deserialize(line)
                    if i == max_location:
                        return
        else:
            for line in self._read_file():
                yield deserialize(line)

    def __iter__(self):
        self.iterator = iter(self._inner_reader())
        return self.iterator

    def __next__(self):
        if not self.iterator:
            self.iterator = iter(self._inner_reader())
        return next(self.iterator)

    def __len__(self):
        return self.length

    def __del__(self):
        try:
            os.remove(self.file)
        except:  # nosec
            pass
