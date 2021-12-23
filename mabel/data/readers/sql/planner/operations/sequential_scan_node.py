"""
Sequential Scan Node

This is a SQL Query Execution Plan Node.

This Node reads and parses the data from a blob into a Relation. This Node doesn't try
to use any Index or ZomeMap to improve performance. This Node is intended to be used
in situations where there there are no relevant Index or ZoneMaps available.

As we read, we'll create a Min/Max index - we won't perform full profiling as this is
currently too slow for real-time use. 
"""
from enum import Enum
from mabel.data.internals.relation import Relation
from mabel.data.readers.internals import decompressors, parsers
from mabel.data.readers.internals.parallel_reader import empty_list
from mabel.utils import paths


class EXTENSION_TYPE(str, Enum):
    # labels for the file extentions
    DATA = "DATA"
    CONTROL = "CONTROL"
    INDEX = "INDEX"

class PUSHDOWN(str, Enum):
    NONE = "NONE"
    SELECTION = "SELECTION"
    PROJECTION = "PROJECTION"


KNOWN_EXTENSIONS = {
    ".txt": (decompressors.block, parsers.pass_thru, EXTENSION_TYPE.DATA, PUSHDOWN.NONE),
    ".json": (decompressors.block, parsers.json, EXTENSION_TYPE.DATA, PUSHDOWN.NONE),
    ".zstd": (decompressors.zstd, parsers.json, EXTENSION_TYPE.DATA, PUSHDOWN.NONE),
    ".lzma": (decompressors.lzma, parsers.json, EXTENSION_TYPE.DATA, PUSHDOWN.NONE),
    ".zip": (decompressors.unzip, parsers.pass_thru, EXTENSION_TYPE.DATA, PUSHDOWN.NONE),
    ".jsonl": (decompressors.lines, parsers.json, EXTENSION_TYPE.DATA, PUSHDOWN.NONE),
    ".xml": (decompressors.block, parsers.xml, EXTENSION_TYPE.DATA, PUSHDOWN.NONE),
    ".lxml": (decompressors.lines, parsers.xml, EXTENSION_TYPE.DATA, PUSHDOWN.NONE),
    ".parquet": (decompressors.parquet, parsers.pass_thru, EXTENSION_TYPE.DATA, PUSHDOWN.PROJECTION),
    ".orc": (decompressors.orc, parsers.pass_thru, EXTENSION_TYPE.DATA, PUSHDOWN.PROJECTION),
    ".csv": (decompressors.csv, parsers.pass_thru, EXTENSION_TYPE.DATA, PUSHDOWN.PROJECTION),
    ".ignore": (empty_list, empty_list, EXTENSION_TYPE.CONTROL, PUSHDOWN.NONE),
    ".complete": (empty_list, empty_list, EXTENSION_TYPE.CONTROL, PUSHDOWN.NONE),
    ".index": (empty_list, empty_list, EXTENSION_TYPE.INDEX, PUSHDOWN.NONE),
}


class SequentialScanNode():

    def __init__(self):
        pass

    def execute(self):



        if self.force_format:
            ext = self.force_format
        else:
            bucket, path, stem, ext = paths.get_parts(blob_name)

        if ext not in KNOWN_EXTENSIONS:
            # if we don't know the file type, return an empty relation
            return Relation()

        decompressor, parser, file_type, push_down = KNOWN_EXTENSIONS[ext]

        # Read
        record_iterator = self.reader.read_blob(blob_name)
        # Decompress
        record_iterator = decompressor(record_iterator)
        # Parse
        record_iterator = map(parser, record_iterator)


        min_max_index = IndexMinMax().build(record_iterator)

        