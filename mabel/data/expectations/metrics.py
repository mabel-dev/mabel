"""

count
min
max
missing
cummulative sum
** unique

string:
- string length


"""
from typing import Any
from pydantic import BaseModel  #type:ignore
import datetime


class Metric(BaseModel):
    minimum: Any = None
    maximum: Any = None
    count: int = 0
    missing: int = 0
    cummulative_sum:float = 0    

class MetricCollector(object):

    def __init__(self):
        self.counter = 0
        self.collector = {}

    def add(self, row):
        for k,v in row.items():
            collector = self.collector.get(k, Metric())
            collector.count += 1

            if v is None:
                collector.missing += 1
                continue

            value_type = type(v)
            if value_type in (int,float,str,datetime.date,datetime.datetime):
                collector.maximum = max(v, collector.maximum or v)
                collector.minimum = min(v, collector.minimum or v)
            if value_type in (int, float):
                collector.cummulative_sum += v

            self.collector[k] = collector