import os
import glob
import shutil
from ...data.writers.internals.base_inner_writer import BaseInnerWriter
from ...utils import paths


class FileWriter(BaseInnerWriter):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def commit(
            self,
            byte_data,
            file_name=None):

        _filename = self._build_path()
        bucket, path, filename, ext = paths.get_parts(_filename)
        if file_name:
            _filename = bucket + '/' + path + '/' + file_name

        os.makedirs(bucket + '/' + path, exist_ok=True)
        with open(_filename, mode='wb') as file:
            file.write(byte_data)

        return _filename
