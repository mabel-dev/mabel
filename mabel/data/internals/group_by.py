from typing import Iterable, Tuple
from cityhash import CityHash32

class GroupBy():

    def __init__(self, dictset, *columns):
        self._dictset = dictset
        if isinstance(columns, (list, set, tuple)):
            self._columns = columns
        else:
            columns = [columns]
        self._group_keys = {}


    def _map(self):
        """
        Create Tuples (GroupID, CollectedColumn, Value)
        """
        for index, record in enumerate(self._dictset):
            group_key = ":".join([str(record.get(column, '')) for column in self._columns])
            group_key = CityHash32(group_key) % 4294967295
            if group_key not in self._group_keys:
                self._group_keys[group_key] = [(column,record.get(column)) for column in self._columns]

            for column in self._columns:
                yield (group_key, column, record.get(column))


    def count(self):
        collector = {}
        for index, record in enumerate(self._map()):
            keys = collector.get(record[0], [])
            keys.append((record[1], record[2],))
            collector[record[0]] = keys

        for k,v in collector.items():

            result = {}
            keys = self._group_keys[k]
            for key in keys:
                result[key[0]] = key[1]
            
            result["count"] = len(v) // len(self._columns)
            yield result
