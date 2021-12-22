import os
from ...data.writers.internals.base_inner_writer import BaseInnerWriter
from ...logging import get_logger
from ...utils import paths


class DiskWriter(BaseInnerWriter):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def commit(self, byte_data, blob_name=None):

        try:
            bucket, path, stem, ext = paths.get_parts(blob_name)

            os.makedirs(bucket + "/" + path, exist_ok=True)
            with open(blob_name, mode="wb") as file:
                file.write(byte_data)

            return blob_name
        except Exception as err:  # pragma: no cover
            get_logger().error(f"Problem saving blob to disk {type(err).__name__}")
            raise err
