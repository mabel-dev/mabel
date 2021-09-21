from abc import ABC, abstractclassmethod


class BaseStorageClass(ABC):

    iterator = None
    length = -1

    @abstractclassmethod
    def __init__(self, iterator):
        pass

    @abstractclassmethod
    def _inner_reader(self, *locations):
        pass

    def __iter__(self):
        self.iterator = self._inner_reader()
        return self.iterator

    def __next__(self):
        if not self.iterator:
            self.iterator = self._inner_reader()
        return next(self.iterator)

    def __len__(self):
        return self.length
