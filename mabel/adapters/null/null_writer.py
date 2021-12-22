"""
Null Writer

Impotent writer for testing, writes to the log to help with debugging.

#nodoc - don't add to the documentation wiki
"""
from ...data.writers.internals.base_inner_writer import BaseInnerWriter


class NullWriter(BaseInnerWriter):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        kwargs_passed = [f"{k}={v!r}" for k, v in kwargs.items()]
        self.formatted_args = ", ".join(kwargs_passed)

    def commit(self, byte_data, blob_name=None):

        return f"NullWriter({blob_name})"
