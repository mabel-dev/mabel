"""
This exception should be used when a lazy import fails
"""
from .base_exception import BaseException


class MissingDependencyError(BaseException):
    pass
