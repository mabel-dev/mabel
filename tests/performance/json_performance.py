"""
JSON parsing and serialization performance tests so a decision on 
which library(s) to use can be made - previously the selection was
inconsistent.

Results (seconds to process 10m rows):

 library | parsing | serialize  
-------------------------------
 json    |    36.6 |      1.74 
 ujson   |    16.5 |      0.86
 orjson  |    10.4 |      0.66   <- lower is better
 simd    |     2.8 |       N/A   <- lower is better
-------------------------------

"""
import time


def _inner_file_reader(
    file_name: str, chunk_size: int = 32 * 1024 * 1024, delimiter: str = b"\n"
):
    """
    This is the guts of the reader - it opens a file and reads through it
    chunk by chunk. This allows huge files to be processed as only a chunk
    at a time is in memory.
    """
    with open(file_name, "rb") as f:
        carry_forward = b""
        chunk = b"INITIALIZED"
        while len(chunk) > 0:
            chunk = f.read(chunk_size)
            augmented_chunk = carry_forward + chunk
            lines = augmented_chunk.split(delimiter)
            carry_forward = lines.pop()
            yield from lines
        if carry_forward:
            yield carry_forward


reader = list(_inner_file_reader("tests/data/tweets/tweets-0000.jsonl")) * 200000
print(len(reader))


def test_parser(parser):
    for item in reader:
        parser(item)


def test_serializer(serializer):
    for item in reader:
        dic = orjson.loads(item)
        serializer(dic)


def test_simd_serializer(serializer):
    for item in reader:
        s = sparser(item)
        dic = s.as_dict()
        dic.mini = s.mini
        setattr(dic, "mini", s.mini)
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

# import cysimdjson
import simdjson
import os
import sys

simparser = simdjson.Parser()


def sparser(o):
    return simparser.parse(o)


def simd_dump(o):
    o.mini


# sys.path.insert(1, os.path.join(sys.path[0], "../.."))

# print("cysimd parse:", time_it(test_parser, parser.parse))
print("pysimd parse:", time_it(test_parser, test_simd_serializer))
# print("json parse  :", time_it(test_parser, json.loads))
print("ujson parse :", time_it(test_parser, ujson.loads))
print("orjson parse:", time_it(test_parser, orjson.loads))  # <- fastest

# print("map", time_it_2())
# print("comp", time_it_3())
# print("for", time_it_4())

# print("json serialize   :", time_it(test_serializer, json.dumps))
print("ujson serializer :", time_it(test_serializer, ujson.dumps))
print("orjson serializer:", time_it(test_serializer, orjson.dumps))  # <- fastest
print("pysimd serializer:", time_it(test_simd_serializer, orjson.dumps))
