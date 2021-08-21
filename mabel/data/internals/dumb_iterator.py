"""
There's nothing clever here, it's just a dumb iterator wrapper
"""

from sys import excepthook


class DumbIterator:
    def __init__(self, i):
        self._i = iter(i)

    def __next__(self):
        # try:
        return next(self._i)
        # except Exception:
        #    raise StopIteration()
