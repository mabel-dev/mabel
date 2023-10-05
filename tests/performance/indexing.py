"""

"""
import sys
import os

sys.path.insert(1, os.path.join(sys.path[0], "../.."))
from mabel.adapters.null import NullWriter
from orso.logging import get_logger
from mabel.data.validator import Schema
from mabel.data import BatchWriter, Reader
from mabel.adapters.disk import DiskWriter, DiskReader
import orjson as json


logger = get_logger()

schema_definition = {
    "fields": [
        {"name": "userid", "type": "numeric"},
        {"name": "username", "type": "string"},
        {"name": "user_verified", "type": "boolean"},
        {"name": "followers", "type": "numeric"},
        {"name": "tweet", "type": "string"},
        {"name": "location", "type": ["string", "nullable"]},
        {"name": "sentiment", "type": "numeric"},
        {"name": "timestamp", "type": "date"},
    ]
}


def read_jsonl(filename, limit=-1, chunk_size=32 * 1024 * 1024, delimiter="\n"):
    """ """
    file_reader = read_file(filename, chunk_size=chunk_size, delimiter=delimiter)
    line = next(file_reader, None)
    while line:
        yield json.loads(line)
        limit -= 1
        if limit == 0:
            return
        try:
            line = next(file_reader)
        except StopIteration:
            return


def read_file(filename, chunk_size=32 * 1024 * 1024, delimiter="\n"):
    """
    Reads an arbitrarily long file, line by line
    """
    with open(filename, "r", encoding="utf8") as f:
        carry_forward = ""
        chunk = "INITIALIZED"
        while len(chunk) > 0:
            chunk = f.read(chunk_size)
            augmented_chunk = carry_forward + chunk
            lines = augmented_chunk.split(delimiter)
            carry_forward = lines.pop()
            yield from lines
        if carry_forward:
            yield carry_forward


schema = Schema(schema_definition)
lines = read_jsonl("tests/data/index/not/tweets.jsonl")

writer = BatchWriter(
    inner_writer=DiskWriter,
    dataset="_temp/idx",
    # schema=schema,
    indexes=["user_name"],
)

for record in lines:
    writer.append(record)
writer.finalize()

reader = Reader(inner_reader=DiskReader, dataset="_temp/idx", filters=("user_name", "==", "Remy"))
i = 0
for i, r in enumerate(reader):
    print(i, r)

print(i)
