from . import BaseStorageClass
import msgpack

"""
Experimentation with this format, it doesn't appear to be good in any
situation so isn't recommended at this point.
"""


class StorageClassMsgPackMemory(BaseStorageClass):
    def __init__(self, iterable):

        self.length = 0
        self.data = []

        for self.length, row in enumerate(iterable):
            if hasattr(row, "as_dict"):
                self.data.append(msgpack.packb(row.as_dict()))
            else:
                self.data.append(msgpack.packb(row))
        self.length = len(self.data)


    def _inner_reader(self, *locations):
        if locations:
            yield from [msgpack.unpackb(self.data[l]) for l in locations]
        else:
            yield from map(msgpack.unpackb, self.data)

    def __iter__(self):
        self.iterator = iter(self._inner_reader())
        return self.iterator

    def __next__(self):
        if not self.iterator:
            self.iterator = iter(self._inner_reader())
        return next(self.iterator)

    def __len__(self):
        return self.length
