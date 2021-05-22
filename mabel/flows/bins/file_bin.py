"""
File Bin
"""
import time
from .base_bin import BaseBin
import os


class FileBin(BaseBin):
    def __init__(self, bin_name: str, path: str):
        self.path = path
        self.name = bin_name
        os.makedirs(self.path, exist_ok=True)

    def __call__(self, record: str, id_: str = ""):

        os.makedirs(f"{self.path}/{self._date_part()}", exist_ok=True)
        filename = f"{self.path}/{self._date_part()}/{id_}{time.time_ns()}.txt"
        with open(filename, "w") as file:
            file.write(record)
        return filename
