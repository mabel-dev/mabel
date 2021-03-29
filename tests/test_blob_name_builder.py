import datetime
import sys
import os
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from mabel.helpers import BlobPaths


def test_blob_paths_split_filename():

    name, ext = BlobPaths.split_filename("one_extention.ext")
    assert name == 'one_extention', f"{name} {ext}"
    assert ext == '.ext', f"{name} {ext}"

    name, ext = BlobPaths.split_filename("two_extention.ext.zip")
    assert name == 'two_extention.ext', f"{name} {ext}"
    assert ext == '.zip', f"{name} {ext}"

    name, ext = BlobPaths.split_filename("double_dot..zip")
    assert name == 'double_dot.', f"{name} {ext}"
    assert ext == '.zip', f"{name} {ext}"

    name, ext = BlobPaths.split_filename("no_ext")
    assert name == 'no_ext', f"{name} {ext}"
    assert ext == '', f"{name} {ext}"

    name, ext = BlobPaths.split_filename(".all_ext")
    assert name == '.all_ext', f"{name} {ext}"
    assert ext == '', f"{name} {ext}"

    name, ext = BlobPaths.split_filename(".dot_start.zip")
    assert name == '.dot_start', f"{name} {ext}"
    assert ext == '.zip', f"{name} {ext}"

    name, ext = BlobPaths.split_filename("")  # empty
    assert len(name) == 0
    assert len(ext) == 0

    name, ext = BlobPaths.split_filename("with/path/file.ext") 
    assert name == 'with/path/file', f"{name} {ext}"
    assert ext == '.ext', f"{name} {ext}"

    name, ext = BlobPaths.split_filename("with/dot.in/path") 
    assert name == 'with/dot.in/path', f"{name} {ext}"
    assert ext == '', f"{name} {ext}"


def test_blob_paths_get_paths():

    bucket, path, name, ext = BlobPaths.get_parts("bucket/parent_folder/sub_folder/filename.ext")

    assert bucket == 'bucket'
    assert name == 'filename'
    assert ext == '.ext'
    assert path == 'parent_folder/sub_folder/'


def test_blob_paths_builder():

    template = 'year_%Y/month_%m/day_%d/%date_%time.%f'
    path = BlobPaths.build_path(template, datetime.datetime(2000, 9, 19, 1, 36, 42, 365))

    assert path == "year_2000/month_09/day_19/2000-09-19_013642.000365"


if __name__ == "__main__":
    test_blob_paths_split_filename()
    test_blob_paths_get_paths()
    test_blob_paths_builder()
