class MabelException(Exception):
    pass


class DataNotFoundError(MabelException):
    pass


class IntegrityError(MabelException):
    pass


class InvalidArgument(MabelException):
    pass


class InvalidCombinationError(MabelException):
    pass


class InvalidDataSetError(MabelException):
    pass


class InvalidReaderConfigError(MabelException):
    pass


class MissingDependencyError(MabelException):
    pass


class UnsupportedTypeError(MabelException):
    pass


class ValidationError(MabelException):
    pass


class InvalidSyntaxError(MabelException):
    pass
