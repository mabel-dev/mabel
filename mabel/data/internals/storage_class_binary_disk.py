"""
StorageClassDisk is a helper class for persisting DictSets locally, it is the backend
for the BINARY_DISK variation of the STORAGE CLASSES.

This stores DICTSETs in a binary format - which should be smaller and faster - but only
supports a subset of field types.
"""
import os
import sys
import mmap
import atexit
import struct
import datetime
from tempfile import NamedTemporaryFile
from typing import Iterator

STRING_LENGTH = 32

empty = lambda x: b'\x00'

def date_dumper(var):
    date = parse_iso(var)
    if date:
        return date.timestamp()
    return None

def nullify(var):
    if var is None:
        return b'\x00'
    return var

def load_float(var):
    if var == sys.float_info.min:
        return None
    return var

def dump_float(var):
    if var is None:
        return sys.float_info.min
    return var

BUFFER_SIZE = 16 * 1024 * 1024  # 16Mb

ENDIAN = "<"

TYPE_STORAGE = {
    "int": "q",
    "float": "d",
    "str": f"{STRING_LENGTH}s",
    "datetime": "d",
    "bool": "?",
    "spacer": "x"
}

TYPE_DUMPERS = {
    "int": lambda x:x,
    "float": dump_float,
    "str": lambda x: str(x)[:STRING_LENGTH].encode(),
    "datetime": date_dumper,
    "bool": lambda x:x
}

TYPE_LOADERS = {
    "int": lambda x:x,
    "float": load_float,
    "str": lambda x: str(x).split('\\x00',1)[0],
    "datetime": lambda x: datetime.datetime.fromtimestamp(x),
    "bool": lambda x:x,
    "empty": None
}


def parse_iso(value):
    DATE_SEPARATORS = {"-", ":"}
    # date validation at speed is hard, dateutil is great but really slow, this is fast
    # but error-prone. It assumes it is a date or it really nothing like a date.
    # Making that assumption - and accepting the consequences - we can convert upto
    # three times faster than dateutil.
    try:
        if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
            return value
        if isinstance(value, str) and len(value) >= 10:
            if not value[4] in DATE_SEPARATORS or not value[7] in DATE_SEPARATORS:
                return None
            if len(value) == 10:
                # YYYY-MM-DD
                return datetime.date(*map(int, [value[:4], value[5:7], value[8:10]]))
            if len(value) >= 16:
                if not value[10] == "T" or not value[13] in DATE_SEPARATORS:
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

    def serialize(self, record: dict) -> bytes:
        return self.constructor(*[nullify(TYPE_DUMPERS.get(v, empty)(record.get(k))) for k,v in self.schema_dict.items()])
#        return struct.pack(self.schema_str, *[nullify(TYPE_DUMPERS.get(v, empty)(record.get(k))) for k,v in self.schema_dict.items()])

    def deserialize(self, record: bytes) -> dict:
        #values = struct.unpack(self.schema_str, record)
        values = self.destructor(record)
        return { d[0]: TYPE_LOADERS[d[1]](values[i]) for i, d in enumerate(self.schema_dict.items()) }

    @staticmethod
    def determine_schema(record: dict) -> dict:
        schema = {}
        for k, v in record.items():
            value_type = type(v).__name__
            if value_type == "str" and parse_iso(v):
                value_type = "datetime"
            if value_type not in TYPE_STORAGE:
                value_type = "spacer"
            schema[k] = value_type
        return schema


    def __init__(self, iterator: Iterator = []):
        try:
            record = next(iterator)
            self.schema_dict = self.determine_schema(record)
            self.schema_str = ENDIAN + ''.join([TYPE_STORAGE.get(v,'x') for k,v in self.schema_dict.items()])
            self.schema_size = struct.calcsize(self.schema_str)
            self.destructor = struct.Struct(self.schema_str).unpack
            self.constructor = struct.Struct(self.schema_str).pack
        except:
            raise

        self.inner_reader = None
        self.length = -1

        self.file = NamedTemporaryFile(prefix="mabel-dictset").name
        atexit.register(os.remove, self.file)

        buffer = bytearray()
        with open(self.file, "wb") as f:
            for self.length, row in enumerate(iterator):
                buffer.extend(self.serialize(row))
                if len(buffer) > (BUFFER_SIZE):
                    f.write(buffer)
                    buffer = bytearray()
            if len(buffer) > 0:
                f.write(buffer)
            f.flush()

        self.length += 1

    def _read_file(self):
        """
        MMAP is by far the fastest way to read files in Python.
        """
        index = 0
        with open(self.file, mode="rb") as file_obj:
            with mmap.mmap(
                file_obj.fileno(), length=0, access=mmap.ACCESS_READ
            ) as mmap_obj:
                line = mmap_obj.read(self.schema_size)
                while line:
                    yield line
                    line = mmap_obj.read(self.schema_size)
                index += 1


    def _inner_reader(self, *locations):
        if locations:
            max_location = max(locations)
            min_location = min(locations)

            reader = self._read_file()

            for i in range(min_location):
                next(reader)

            for i, line in enumerate(reader, min_location):
                if i in locations:
                    yield self.deserialize(line)
                    if i == max_location:
                        return
        else:
            for line in self._read_file():
                yield self.deserialize(line)

    def __iter__(self):
        return self._inner_reader()

    def __next__(self):
        return next(self)

    def __len__(self):
        return self.length

    def __del__(self):
        try:
            os.remove(self.file)
        except: # nosec
            pass


"""

def unpack_from(fmt, data, offset = 0):
    (byte_order, fmt, args) = (fmt[0], fmt[1:], ()) if fmt and fmt[0] in ('@', '=', '<', '>', '!') else ('@', fmt, ())
    fmt = filter(None, re.sub("p", "\tp\t",  fmt).split('\t'))
    for sub_fmt in fmt:
        if sub_fmt == 'p':
            (str_len,) = struct.unpack_from('B', data, offset)
            sub_fmt = str(str_len + 1) + 'p'
            sub_size = str_len + 1
        else:
            sub_fmt = byte_order + sub_fmt
            sub_size = struct.calcsize(sub_fmt)
        args += struct.unpack_from(sub_fmt, data, offset)
        offset += sub_size
    return args


def pack(fmt, *args):
    (byte_order, fmt, data) = (fmt[0], fmt[1:], '') if fmt and fmt[0] in ('@', '=', '<', '>', '!') else ('@', fmt, '')
    fmt = filter(None, re.sub("p", "\tp\t",  fmt).split('\t'))
    for sub_fmt in fmt:
        if sub_fmt == 'p':
            (sub_args, args) = ((args[0],), args[1:]) if len(args) > 1 else ((args[0],), [])
            sub_fmt = str(len(sub_args[0]) + 1) + 'p'
        else:
            (sub_args, args) = (args[:len(sub_fmt)], args[len(sub_fmt):])
            sub_fmt = byte_order + sub_fmt
        data += struct.pack(sub_fmt, *sub_args)
    return data
"""