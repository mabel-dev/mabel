"""
Results (seconds to search for a username in 65,500 rows):

 indexed | row exists | time
-------------------------------
 yes     |    yes     |   0.094   <- about 3.5x faster when is match
 yes     |    no      |   0.006   <- over 50x faster when no match
 no      |    yes     |   0.357
 no      |    no      |   0.332
-------------------------------
"""

import os, sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))
from mabel.data.internals.index import Index, IndexBuilder
import time


def time_it(dataset, username):
    start = time.perf_counter_ns()
    reader = Reader(
        inner_reader=DiskReader,
        dataset=dataset,
        raw_path=True,
        filters=("user_name", "==", username),
    )
    res = [r for r in reader]
    print(len(res))
    return (time.perf_counter_ns() - start) / 1e9


import os, sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))
from mabel.data import Reader
from mabel.adapters.disk import DiskReader
from orso.logging import get_logger

get_logger().setLevel(100)

user_name = "Verizon Support"

print("indexed\t:", time_it("tests/data/index/is", user_name))
print("not indexed\t:", time_it("tests/data/index/not", user_name))
print("indexed\t:", time_it("tests/data/index/is", user_name + "bb"))
print("not indexed\t:", time_it("tests/data/index/not", user_name + "bb"))


reader = Reader(
    inner_reader=DiskReader,
    dataset="tests/data/index/is",
    raw_path=True,
)
idx = Index.build_index(reader, "user_name")
idx.dump("tests/data/index/is/tweets.jsonl.user_name.idx")
