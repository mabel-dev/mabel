"""
Testing writer performance after observing some jobs which
were a previously a few minutes were observed to take over an hour.

Results (average of 10 runs, 100,000 rows of 8 field record, NullWriter)
┌─────────────┬────────────┬────────┬───────┬─────────────┐
│ compression │ validation │  time  │ ratio │ rows/second │
├─────────────┼────────────┼────────┼───────┼─────────────┤
│    clear    │   False    │ 0.195  │ 0.999 │    294288   │
│     zstd    │   False    │ 0.252  │ 0.774 │    228039   │
│     lzma    │   False    │ 10.119 │ 0.019 │     5689    │
│    clear    │    True    │ 0.606  │ 0.322 │    95001    │
│     zstd    │    True    │ 0.717  │ 0.272 │    80203    │
│     lzma    │    True    │ 10.876 │ 0.017 │     5293    │
└─────────────┴────────────┴────────┴───────┴─────────────┘
Your numbers will vary depending on any number of factors
"""
import sys
import os
import time
import statistics

sys.path.insert(1, os.path.join(sys.path[0], "../.."))
from mabel.data import BatchWriter
from mabel.adapters.null import NullWriter
from mabel.logging import get_logger
from mabel.data.validator import Schema
from mabel.data.formats import display, dictset

import ujson as json


logger = get_logger()
logger.setLevel(100)

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


def execute_test(compress, schema, reader):

    # reader = read_jsonl('tweets.jsonl')

    res = []
    for i in range(10):
        writer = BatchWriter(
            inner_writer=NullWriter,
            dataset="{datefolders}",
            format=compress,
            schema=schema,
        )
        start = time.perf_counter_ns()
        for record in reader:
            writer.append(record)
        writer.finalize()
        res.append((time.perf_counter_ns() - start) / 1e9)
    return statistics.mean(res)


schema = Schema(schema_definition)
lines = list(read_jsonl("tests/data/formats/tweets.jsonl"))

print(len(lines))

results = []
result = {
    "format": "jsonl",
    "validation": False,
    "time": execute_test("jsonl", None, lines),
}
results.append(result)

result = {
    "format": "jsonl",
    "validation": True,
    "time": execute_test("jsonl", schema, lines),
}
results.append(result)

result = {
    "format": "zstd",
    "validation": False,
    "time": execute_test("zstd", None, lines),
}
results.append(result)

result = {
    "format": "zstd",
    "validation": True,
    "time": execute_test("zstd", schema, lines),
}
results.append(result)

result = {
    "format": "lzma",
    "validation": False,
    "time": execute_test("lzma", None, lines),
}
results.append(result)

result = {
    "format": "lzma",
    "validation": True,
    "time": execute_test("lzma", schema, lines),
}
results.append(result)

result = {
    "format": "parquet",
    "validation": False,
    "time": execute_test("parquet", None, lines),
}
results.append(result)

result = {
    "format": "parquet",
    "validation": True,
    "time": execute_test("parquet", schema, lines),
}
results.append(result)

fastest = 100000000000
for result in results:
    if result["time"] < fastest:
        fastest = result["time"]
results = dictset.set_column(
    results, "ratio", lambda r: int((1000 * fastest) / r["time"]) / 1000
)
results = dictset.set_column(
    results, "rows/second", lambda r: int(len(lines) / r["time"])
)
results = dictset.set_column(results, "time", lambda r: int(1000 * r["time"]) / 1000)

print(display.ascii_table(results, 100))
