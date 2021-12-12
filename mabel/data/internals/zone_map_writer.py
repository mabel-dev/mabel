import datetime
from typing import Any
from mabel.data.internals.algorithms.hyper_log_log import HyperLogLog


# This should require a filter which takes about 1.8Mb of memory and uses 9 hashes
BLOOM_FILTER_SIZE = 1000000
BLOOM_FILTER_FALSE_POSITIVE_RATE = 0.001


class ZoneMap:
    def __init__(self):
        self.type: str = "unknown"
        self.minimum: Any = None
        self.maximum: Any = None
        self.count: int = 0
        self.missing: int = 0
        self.cumulative_sum: float = 0
        self.unique_items: int = 0
        self.cardinality: float = 0

    def dict(self, column: str):
        return {
            column: {
                "type": self.type,
                "minimum": self.minimum,
                "maximum": self.maximum,
                "count": self.count,
                "missing": self.missing,
                "cumulative_sum": self.cumulative_sum
            }
        }


class ZoneMapWriter(object):
    def __init__(self):
        self.collector = {}
        self.hyper_log_logs = {}
        self.record_counter = 0

    def add(self, row):
        self.record_counter += 1

        for k, v in row.items():

            collector = self.collector.get(k)
            if not collector:
                collector = ZoneMap()
                #self.bloom_filters[k] = BloomFilter(
                #    BLOOM_FILTER_SIZE, BLOOM_FILTER_FALSE_POSITIVE_RATE
                #)
                self.hyper_log_logs[k] = HyperLogLog(0.01)
            collector.count += 1

            # if the value is missing, skip anything else
            if (v or "") == "" and v != False:
                continue

            # calculate the min/max for ordinals (and strings) and the cummulative
            # sum for numerics
            value_type = type(v)
            value_type_name = value_type.__name__
            if value_type in (int, float, str, datetime.date, datetime.datetime):
                collector.maximum = max(v, collector.maximum or v)
                collector.minimum = min(v, collector.minimum or v)
            if value_type in (int, float):
                collector.cumulative_sum += v

            # track the type of the field
            if collector.type != value_type_name:
                if collector.type == "unknown":
                    collector.type = value_type_name
                elif collector.type in ("float", "int") and value_type_name in (
                    "float",
                    "int",
                ):
                    collector.type = "numeric"
                else:
                    collector.type = "mixed"

            # count the unique items, use a bloom filter to save space - we're going to
            # use to create estimates of value distribution, so this is probably okay.
            #if self.bloom_filters[k].add(v):
            #    collector.unique_items += 1
            
            self.hyper_log_logs[k].add(v)

            # put the profile back in the collector
            self.collector[k] = collector

    def profile(self):
        """
        Pass the record count in as we can't count the rows ourselves
        """
        for column in self.collector:
            profile = self.collector[column].dict(column)
            profile[column]['missing'] = self.record_counter - profile[column]['count']
            profile[column]['cardinality'] = self.hyper_log_logs[column].card() / profile[column]['count']

            yield profile
