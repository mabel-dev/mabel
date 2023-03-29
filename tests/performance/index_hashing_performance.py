import os, sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))
from mabel.data import Reader
from mabel.data.internals.index import IndexBuilder
from mabel.logging import get_logger
from timer import Timer
import orjson

get_logger().setLevel(100)


def _inner_file_reader(file_name: str, chunk_size: int = 32 * 1024 * 1024, delimiter: str = "\n"):
    """
    This is the guts of the reader - it opens a file and reads through it
    chunk by chunk. This allows huge files to be processed as only a chunk
    at a time is in memory.
    """
    with open(file_name, "r", encoding="utf8") as f:
        carry_forward = ""
        chunk = "INITIALIZED"
        while len(chunk) > 0:
            chunk = f.read(chunk_size)
            augmented_chunk = carry_forward + chunk
            lines = augmented_chunk.split(delimiter)
            carry_forward = lines.pop()
            yield from map(orjson.loads, lines)
        if carry_forward:
            yield orjson.loads(carry_forward)


reader = list(_inner_file_reader("tests/data/tweets/tweets-0000.jsonl")) * 10000
print(len(reader))

ib = IndexBuilder("tweet")
with Timer():
    for i, r in enumerate(reader):
        ib.add(i, r)
