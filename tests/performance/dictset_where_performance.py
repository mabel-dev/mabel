#
#  for 1 million iterations of 50 records:
#
#   old dictset : 6.69
#   filter      : 5.29
import datetime
import time
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))
from mabel import Reader
from mabel.adapters.disk import DiskReader
from mabel.data.formats import dictset

try:
    from rich import traceback

    traceback.install()
except ImportError:  # pragma: no cover
    pass


def where_clause(row):
    return row["username"] == "BBCNews"


def get_data():
    """ensure we can read the test files"""
    r = Reader(inner_reader=DiskReader, dataset="tests/data/tweets")
    return list(r) * 1000000


def time_it(test, *args):
    start = time.perf_counter_ns()
    test(*args)
    return (time.perf_counter_ns() - start) / 1e9


def use_dictset(data, cycles):
    list(dictset.select_from(data, where=where_clause))


def use_filter(data, cycles):
    list(filter(where_clause, data))


def use_comprehension(data, cycle):
    ([item for item in data if where_clause(item)])


def just_code(data, cycles):
    r = []
    for row in data:
        if where_clause(row):
            r.append(r)


data = get_data()

cycles = 1
print("use_dictset :", time_it(use_dictset, data, cycles))  # 5.0
print("use_filter  :", time_it(use_filter, data, cycles))  # 4.5 faster
print("just code   :", time_it(just_code, data, cycles))  # 5.8
print("comprehens  :", time_it(use_comprehension, data, cycles))  # 5.8
