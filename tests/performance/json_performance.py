"""
JSON parsing and serialization performance tests so a decision on 
which library(s) to use can be made - previously the selection was
inconsistent.

Results (seconds to process 250,000 rows):

 library | parsing | serialize  
-------------------------------
 json    |    1.08 |      1.74 
 ujson   |    0.52 |      0.86
 orjson  |    0.40 |      0.66   <- lower is better
-------------------------------

"""
import time


def _inner_file_reader(
    file_name: str, chunk_size: int = 32 * 1024 * 1024, delimiter: str = "\n"
):
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
            yield from lines
        if carry_forward:
            yield carry_forward


reader = list(_inner_file_reader("tests/data/tweets/tweets-0000.jsonl")) * 10000
print(len(reader))


def test_parser(parser):
    for item in reader:
        parser(item)


def test_serializer(serializer):
    for item in reader:
        dic = orjson.loads(item)
        serializer(dic)


def time_it(test, *args):
    start = time.perf_counter_ns()
    test(*args)
    return (time.perf_counter_ns() - start) / 1e9


def time_it_2():
    start = time.perf_counter_ns()
    for r in map(orjson.loads, reader):
        pass
    return (time.perf_counter_ns() - start) / 1e9


def time_it_3():
    start = time.perf_counter_ns()
    [orjson.loads(r) for r in reader]
    return (time.perf_counter_ns() - start) / 1e9


def time_it_4():
    start = time.perf_counter_ns()
    for r in reader:
        orjson.loads(r)
    return (time.perf_counter_ns() - start) / 1e9


import json
import ujson
import orjson
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))
import mabel.data.formats.json

print("json parse  :", time_it(test_parser, json.loads))
print("ujson parse :", time_it(test_parser, ujson.loads))
print("orjson parse:", time_it(test_parser, orjson.loads))  # <- fastest
print("mabel parse:", time_it(test_parser, mabel.data.formats.json.parse))
print("map", time_it_2())
print("comp", time_it_3())
print("for", time_it_4())

print("json serialize   :", time_it(test_serializer, json.dumps))
print("ujson serializer :", time_it(test_serializer, ujson.dumps))
print("orjson serializer:", time_it(test_serializer, orjson.dumps))  # <- fastest
print("mabel serializer:", time_it(test_serializer, mabel.data.formats.json.serialize))
