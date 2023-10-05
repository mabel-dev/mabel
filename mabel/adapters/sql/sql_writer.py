from mabel.data.writers.internals.base_inner_writer import BaseInnerWriter


class SqlWriter(BaseInnerWriter):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        kwargs_passed = [f"{k}={v!r}" for k, v in kwargs.items()]
        self.formatted_args = ", ".join(kwargs_passed)

    def commit(self, byte_data, override_blob_name=None):
        # if we've been given the filename, use that, otherwise get the
        # name from the path builder
        if override_blob_name:  # prgama: no cover
            blob_name = override_blob_name
        else:
            blob_name = self._build_path()

        return f"NullWriter({blob_name})"
