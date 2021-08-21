"""
┌──────────────────────┬────────────┐
│       function       │    time    │
├──────────────────────┼────────────┤
│   write_file_clear   │ 1.2371213  │
│ write_file_zstandard │ 0.41138138 │
│   read_file_clear    │ 0.25498286 │
│ read_file_zstandard  │ 0.14555598 │
└──────────────────────┴────────────┘

Results indicate that if you're IO bound (you almost definitely are) that the
effort to compress is paid back many times by the effort to IO.
"""
import os
import time
import lzma
import zstandard
import statistics
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))
import mabel


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


def read_file_lzma(filename=""):
    with lzma.open(filename, "r") as f:  # type: ignore
        yield from f


def read_file_zstandard(filename=""):
    with zstandard.open(filename, "r") as f:  # type: ignore
        yield from f


def write_file_clear(filename="", content=[]):
    with open(filename, "w") as f:
        for row in content:
            f.write(row + "/n")
    return []


def write_file_lzma(filename="", content=[]):
    with lzma.open(filename, "w") as f:
        for row in content:
            f.write(row.encode() + b"/n")
    return []


def write_file_zstandard(filename="", content=[]):
    with zstandard.open(filename, "w") as f:
        for row in content:
            f.write(row + "/n")
    return []


lines = list(read_file_clear("tests/data/formats/jsonl/tweets.jsonl"))
os.makedirs("_temp", exist_ok=True)


def execute_test(func, **kwargs):
    runs = []

    for i in range(5):
        start = time.perf_counter_ns()
        [r for r in func(**kwargs)]
        runs.append((time.perf_counter_ns() - start) / 1e9)

    return statistics.mean(runs)


results = []

result = {
    "function": "write_file_clear",
    "time": execute_test(write_file_clear, filename="_temp/wfc.txt", content=lines),
}
results.append(result)
#result = {
#    "function": "write_file_lzma",
#    "time": execute_test(write_file_lzma, filename="_temp/wfl.lzma", content=lines),
#}
#results.append(result)
result = {
    "function": "write_file_zstandard",
    "time": execute_test(
        write_file_zstandard, filename="_temp/wfz.zstd", content=lines
    ),
}
results.append(result)
result = {
    "function": "read_file_clear",
    "time": execute_test(read_file_clear, filename="_temp/wfc.txt"),
}
results.append(result)
#result = {
#    "function": "read_file_lzma",
#    "time": execute_test(read_file_lzma, filename="_temp/wfl.lzma"),
#}
#results.append(result)
result = {
    "function": "read_file_zstandard",
    "time": execute_test(read_file_zstandard, filename="_temp/wfz.zstd"),
}
results.append(result)

print(mabel.data.internals.display.ascii_table(results, 100))
