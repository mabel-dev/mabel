import decimal

from abc import ABC

import orjson


class BaseStorageClass(ABC):

    iterator = None
    length = -1

    def __init__(self, iterator):
        raise NotImplementedError()

    def _inner_reader(self, *locations):
        raise NotImplementedError()

    def __iter__(self):
        self.iterator = self._inner_reader()
        return self.iterator

    def __next__(self):
        if not self.iterator:
            self.iterator = self._inner_reader()
        return next(self.iterator)

    def __len__(self):
        return self.length

    def parse_json(self, ds):
        return orjson.loads(ds)

    def dump_json(self, ds):
        def handler(obj):
            if isinstance(obj, decimal.Decimal):
                return str(obj)
            raise TypeError

        return orjson.dumps(ds, default=handler)
