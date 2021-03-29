from ..helpers import BlobPaths
from os.path import exists
import os
import shutil
from typing import Optional
import datetime


def file_writer(
        source_file_name: str,
        target_path: str,
        date: Optional[datetime.date] = None,
        add_extention: str = '',
        **kwargs):
    
    if date is None:
        date = datetime.datetime.today()

    filename, extention = BlobPaths.split_filename(target_path)

    # avoid collisions
    collision_tests = 0
    maybe_colliding_filename = BlobPaths.build_path(f"{filename}-{collision_tests:04d}{extention}{add_extention}", date)
    while exists(maybe_colliding_filename):
        collision_tests += 1
        maybe_colliding_filename = BlobPaths.build_path(f"{filename}-{collision_tests:04d}{extention}{add_extention}", date)
    unique_filename = maybe_colliding_filename

    bucket, path, filename, ext = BlobPaths.get_parts(unique_filename)
    os.makedirs(bucket + '/' + path, exist_ok=True)
    
    # save
    return shutil.copy(source_file_name, unique_filename)
