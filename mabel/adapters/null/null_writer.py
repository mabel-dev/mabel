"""
Null Writer

Impotent writer for testing, writes to the log to help with debugging.

#nodoc - don't add to the documentation wiki
"""
from ...logging import get_logger
from ...utils import paths
from ...data.writers.internals.base_inner_writer import BaseInnerWriter


class NullWriter(BaseInnerWriter):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        kwargs_passed = [f"{k}={v!r}" for k, v in kwargs.items()]
        self.formatted_args = ", ".join(kwargs_passed)

    def commit(
            self,
            byte_data,
            file_name=None):

        _filename = self._build_path()
        bucket, path, filename, ext = paths.get_parts(_filename)
        if file_name:
            _filename = bucket + '/' + path + '/' + file_name

        get_logger().debug(f'null_writer({self.formatted_args}, {_filename})')
        return "NullWriter"
