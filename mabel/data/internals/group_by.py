from typing import Iterable, Tuple
from cityhash import CityHash32
import operator

AGGREGATORS = {
    "SUM": operator.add,
    "MAX": max,
    "MIN": min,
    "COUNT": lambda x, y: x + 1
}

class GroupBy():

    def __init__(self, dictset, *columns):
        self._dictset = dictset
        if isinstance(columns, (list, set, tuple)):
            self._columns = columns
        else:
            columns = [columns]
        self._group_keys = {}


    def _map(self, *collect_columns):
        """
        Create Tuples (GroupID, CollectedColumn, Value)
        """
        for index, record in enumerate(self._dictset):
            group_key = ":".join([str(record.get(column, '')) for column in self._columns])
            group_key = CityHash32(group_key) % 4294967295
            if group_key not in self._group_keys:
                self._group_keys[group_key] = [(column,record.get(column)) for column in self._columns]

            for column in collect_columns:
                yield (group_key, column, record.get(column))


    def aggregate(self, aggregations):
        if not isinstance(aggregations, list):
            aggregations = [aggregations]
        if not all(isinstance(agg, tuple) for agg in aggregations):
            raise ValueError("`aggregate` expects a list of Tuples")

        columns_to_collect = [col for func, col in aggregations]

        collector = {}
        for index, record in enumerate(self._map(*columns_to_collect)):
            for func, col in aggregations:

                key = f"{func}({col})"

                existing = collector.get(record[0], {}).get(key)
                value = record[2]

                try:
                    value = float(value)
                    value = int(value)
                except:
                    pass

                if existing:
                    value = AGGREGATORS[func](existing, value)
                elif func == "COUNT":
                    value = 1

                if record[0] not in collector:
                    collector[record[0]] = {}
                collector[record[0]][key] = value

        collector = dict(sorted(collector.items()))

        for group, results in collector.items():

            keys = self._group_keys[group]
            for key in keys:
                results[key[0]] = key[1]
            
            yield results
