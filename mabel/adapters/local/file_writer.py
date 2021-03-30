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
            source_file_name):

        _filename = self._build_path()
        bucket, path, filename, ext = paths.get_parts(_filename)
        os.makedirs(bucket + '/' + path, exist_ok=True)
        return shutil.copy(source_file_name, _filename)
