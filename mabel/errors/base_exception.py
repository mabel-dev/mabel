"""
Base for new exception types.

#nodoc - don't add to the documentation wiki
"""


class BaseException(Exception):
    def __call__(self, *args):  # pragma: no cover
        return self.__class__(*(self.args + args))
