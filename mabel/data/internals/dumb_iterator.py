class DumbIter:
    def __init__(self, i):
        self._i = iter(i)

    def __next__(self):
        return next(self._i)