from .render_error_stack import render_error_stack


class BaseException(Exception):
    def __call__(self, *args):  # pragma: no cover
        return self.__class__(*(self.args + args))


class DataNotFoundError(BaseException):
    pass


class IntegrityError(BaseException):
    pass


class InvalidArgument(BaseException):
    pass


class InvalidCombinationError(BaseException):
    pass


class InvalidDataSetError(BaseException):
    pass


class InvalidReaderConfigError(BaseException):
    pass


class InvalidSyntaxError(BaseException):
    pass


class MissingDependencyError(BaseException):
    pass


class ValidationError(BaseException):
    pass
