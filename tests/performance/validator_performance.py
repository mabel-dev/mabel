"""
┌──────────┬────────────────────┐
│ function │        time        │
├──────────┼────────────────────┤
│ typhaon  │     1.03489932     │
│ pydantic │ 2.1748749800000002 │
└──────────┴────────────────────┘
"""
import cProfile
import time
import pydantic
import datetime
import statistics
from typing import Optional
from pydantic import create_model
import shutil
import sys
import os

sys.path.insert(1, os.path.join(sys.path[0], "../.."))
from mabel.data.validator import Schema
from mabel.data.formats import dictset
from mabel.data.formats.dictset import display

try:
    import orjson as json
except ImportError:
    import ujson as json


py_tweet = {
    "userid": (str, ""),
    "username": (str, ""),
    "user_verified": (bool, False),
    "followers": (int, -1),
    "tweet": (str, ""),
    "location": (Optional[str], None),
    "sentiment": (int, 0),
    "timestamp": (Optional[datetime.datetime], None),
}

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

# random tweet with no sensitive or creative information
data = {
    "userid": 12681557490473,
    "username": "USAgovernmentu1",
    "user_verified": False,
    "followers": 16,
    "tweet": 'The United States is currently a Democracy\nThe leader is known as "President"\nThe current President is Donald John Trump',
    "location": None,
    "sentiment": 0.05555555555555555,
    "timestamp": "2020-12-01T00:00:02",
}


def dict_model(name: str, dict_def: dict):
    fields = {}
    for field_name, value in dict_def.items():
        if isinstance(value, tuple):
            fields[field_name] = value
        elif isinstance(value, dict):
            fields[field_name] = (dict_model(f"{name}_{field_name}", value), ...)
        else:
            raise ValueError(f"Field {field_name}:{value} has invalid syntax")
    return create_model(name, **fields)


TweetModel = dict_model("TweetModel", py_tweet)


def execute_test(func, **kwargs):
    runs = []

    for i in range(5):
        start = time.perf_counter_ns()
        func(**kwargs)
        runs.append((time.perf_counter_ns() - start) / 1e9)

    return statistics.mean(runs)


def jj_validate(cycles=0):
    s = Schema(schema_definition)
    for i in range(cycles):
        s.validate(data)


def py_validate(cycles=0):
    for i in range(cycles):
        m = TweetModel(**data)


cycles = 100000
results = []
result = {"function": "typhaon", "time": execute_test(jj_validate, cycles=cycles)}
results.append(result)
result = {"function": "pydantic", "time": execute_test(py_validate, cycles=cycles)}
results.append(result)


print(display.ascii_table(results, 100))
