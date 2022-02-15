import os
import sys

import orjson

sys.path.insert(1, os.path.join(sys.path[0], "../.."))
from mabel.data.internals.relation import Relation
from mabel.data import STORAGE_CLASS
from timer import Timer


def read_file_clear(filename="", chunk_size=32 * 1024 * 1024, delimiter="\n"):
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


def en(ls):
    for l in ls:
        yield orjson.loads(l)


lines = list(en(read_file_clear("tests/data/formats/jsonl/tweets.jsonl")))
print(len(lines))
os.makedirs("_temp", exist_ok=True)


with Timer("MEMORY"):
    t = Relation(lines, storage_class=STORAGE_CLASS.MEMORY)
    for i, r in enumerate(t):
        pass
    print(i)

with Timer("DISK WRITE"):
    t = Relation(lines, storage_class=STORAGE_CLASS.DISK)
    print(t.count())

with Timer("DISK READ"):
    for i, r in enumerate(t):
        pass
    print(i)

with Timer("COMBINED"):
    t = Relation(lines, storage_class=STORAGE_CLASS.DISK)
    for i, r in enumerate(t):
        pass
    print(i)

import time

time.sleep(1)
