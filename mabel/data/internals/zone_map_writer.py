import datetime
from typing import Any
from mabel.data.internals.algorithms.hyper_log_log import HyperLogLog
from mabel.data.internals.attribute_domains import get_coerced_type


HYPERLOGLOG_ERROR_RATE = 0.005


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
                "type": get_coerced_type(self.type),
                "minimum": self.minimum,
                "maximum": self.maximum,
                "count": self.count,
                "missing": self.missing,
                "cumulative_sum": self.cumulative_sum,
            }
        }


class ZoneMapWriter(object):
    def __init__(self):
        self.collector = {}
        self.hyper_log_logs = {}
        self.record_counter = 0

    def add(self, row):
        # count every time we've been called - this is the total record count
        self.record_counter += 1

        for k, v in row.items():

            collector = self.collector.get(k)
            if not collector:
                collector = ZoneMap()
                # we don't want to put the HLL in the ZoneMap, so create
                # a sidecar HLL which we dispose of later.
                self.hyper_log_logs[k] = HyperLogLog(HYPERLOGLOG_ERROR_RATE)
            collector.count += 1

            # if the value is missing, count it and skip everything else
            if (v or "") == "" and v != False:
                continue

            # calculate the min/max for ordinals (and strings) and the cummulative
            # sum for numerics
            value_type = type(v)
            if value_type in (int, float, str, datetime.date, datetime.datetime):
                collector.maximum = max(v, collector.maximum or v)
                collector.minimum = min(v, collector.minimum or v)
            if value_type in (int, float):
                collector.cumulative_sum += v

            # track the type of the attribute, if it changes mark as mixed
            value_type_name = value_type.__name__
            if collector.type != value_type_name:
                if collector.type == "unknown":
                    collector.type = value_type_name
                else:
                    collector.type = "mixed"

            # count the unique items, use a hyper-log-log for size and speed
            # this gives us an estimate only.
            self.hyper_log_logs[k].add(v)

            # put the profile back in the collector
            self.collector[k] = collector

    def profile(self):
        """
        Pass the record count in as we can't count the rows ourselves
        """
        for column in self.collector:
            profile = self.collector[column].dict(column)
            profile[column]["missing"] = self.record_counter - profile[column]["count"]
            # High cardinality (closer to 1) indicates a greated number of unique
            # values. The error ratio for the HLL is 1/200, so we're going to round to
            # the nearest 1/1000th
            profile[column]["cardinality"] = round(
                self.hyper_log_logs[column].card() / profile[column]["count"], 3
            )

            yield profile