"""
Base for new exception types.

#nodoc - don't add to the documentation wiki
"""


class BaseException(Exception):
    def __call__(self, *args):
        return self.__class__(*(self.args + args))
