"""
Tests for paths to ensure the split and join methods
of paths return the expected values for various
stimulus.
"""

import datetime
import sys
import os
import pathlib

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.utils import paths
from rich import traceback

traceback.install()


def test_blob_paths_get_paths():
    # ignore the filename
    bucket, path, name, ext = paths.get_parts("bucket/parent_folder/sub_folder/filename.ext")
    assert bucket == "bucket"
    assert name == "filename"
    assert ext == ".ext"
    assert path == str(pathlib.PurePosixPath("parent_folder") / "sub_folder") + "/", path

    # with a / at the end
    bucket, path, name, ext = paths.get_parts("bucket/parent_folder/sub_folder/")
    assert bucket == "bucket"
    assert name is None
    assert ext is None
    assert path == str(pathlib.PurePosixPath("parent_folder") / "sub_folder") + "/", path

    # without a / at the end
    bucket, path, name, ext = paths.get_parts("bucket/parent_folder/sub_folder")
    assert bucket == "bucket"
    assert name is None
    assert ext is None
    assert path == str(pathlib.PurePosixPath("parent_folder") / "sub_folder") + "/", path


def test_blob_paths_builder():
    # without trailing /, the / should be added
    template = "year_{yyyy}/month_{mm}/day_{dd}/{yyyy}-{mm}-{dd}/{HH}"
    path = paths.build_path(template, datetime.datetime(2000, 9, 19, 1, 36, 42, 365))
    assert (
        path
        == str(pathlib.PurePosixPath("year_2000") / "month_09" / "day_19" / "2000-09-19" / "01")
        + "/"
    )

    # with trailing /, the / should be retained
    template = "year_{yyyy}/month_{mm}/day_{dd}/{yyyy}-{mm}-{dd}/{HH}/"
    path = paths.build_path(template, datetime.datetime(2000, 9, 19, 1, 36, 42, 365))
    assert (
        path
        == str(pathlib.PurePosixPath("year_2000") / "month_09" / "day_19" / "2000-09-19" / "01")
        + "/"
    )


if __name__ == "__main__":  # pragma: no cover
    from tests.helpers.runner import run_tests

    run_tests()
