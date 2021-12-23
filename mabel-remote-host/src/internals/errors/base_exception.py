"""
Base for new exception types.
"""


class BaseException(Exception):  # pragma: no cover
    def __call__(self, *args):
        return self.__class__(*(self.args + args))
