import os
from ...data.writers.internals.base_inner_writer import BaseInnerWriter
from ...logging import get_logger
from ...utils import paths


class DiskWriter(BaseInnerWriter):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def commit(self, byte_data, override_blob_name=None):

        try:
            # if we've been given the filename, use that, otherwise get the
            # name from the path builder
            if override_blob_name:
                blob_name = override_blob_name
            else:
                blob_name = self._build_path()

            bucket, path, stem, ext = paths.get_parts(blob_name)

            os.makedirs(bucket + "/" + path, exist_ok=True)
            with open(blob_name, mode="wb") as file:
                file.write(byte_data)

            return blob_name
        except Exception as err:  # pragma: no cover
            get_logger().error(f"Problem saving blob to disk {type(err).__name__}")
            raise err
