"""
Filesystem Implementation of a reader.

This is a simple implementation of a reader, created primarily to test the
Reader class. It acts similarly to the blob_reader - it will iterate over
a set of folders, defined by a template and a daterange, read through each
of the files in those folders and present the content back, line by line.
"""
from typing import Iterator, Tuple, Optional, List
import datetime
from ..helpers.blob_paths import BlobPaths
import lzma


def _find_files_at_path(path: str, extention: str) -> List[str]:
    """ Helper function to get a list of files in a given path """
    from os import listdir
    from os.path import isfile, join, exists
    if exists(path):  # skip non-existant folders
        return [join(path, f) for f in listdir(path) if isfile(join(path, f)) and extention in f]
    return []


def _inner_file_reader(
        file_name: str,
        chunk_size: int,
        delimiter: str = "\n"):
    """
    This is the guts of the reader - it opens a file and reads through it
    chunk by chunk. This allows huge files to be processed as only a chunk
    at a time is in memory.
    """
    with open(file_name, 'r', encoding="utf8") as f:
        carry_forward = ""
        chunk = "INITIALIZED"
        while len(chunk) > 0:
            chunk = f.read(chunk_size)
            augmented_chunk = carry_forward + chunk
            lines = augmented_chunk.split(delimiter)
            carry_forward = lines.pop()
            yield from lines
        if carry_forward:
            yield carry_forward


def _inner_compressed_file_reader(file_name: str):
    with lzma.open(file_name, 'r') as f:
        yield from f.readlines()


def file_reader(
        path: str = "",
        chunk_size: int = 8*1024*1024,  # 8Mb
        date_range: Tuple[Optional[datetime.date], Optional[datetime.date]] = (None, None),
        extention: str = '.jsonl',
        delimiter: str = "\n") -> Iterator:

    # if dates aren't provided, use today
    start_date, end_date = date_range
    if not end_date:
        end_date = datetime.date.today()
    if not start_date:
        start_date = datetime.date.today()

    # cycle through each day in the range
    for cycle in range(int((end_date - start_date).days) + 1):
        cycle_date = start_date + datetime.timedelta(cycle)
        # build the path name - it says 'blob' but works for filesystems
        cycle_path = BlobPaths.build_path(path=path, date=cycle_date)
        # get the list of files at that path
        files_at_path = _find_files_at_path(path=cycle_path, extention=extention)
        # for each file, read it and return the rows
        for file in files_at_path:
            if file.endswith('.lzma'):
                reader = _inner_compressed_file_reader(file_name=file)
            else:
                reader = _inner_file_reader(file_name=file, chunk_size=chunk_size)
            yield from reader
