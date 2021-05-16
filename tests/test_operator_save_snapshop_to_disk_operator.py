import os
import sys
import shutil

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.operators.disk import DiskBatchWriterOperator
from rich import traceback

traceback.install()


def test_save_to_disk_operator():

    shutil.rmtree("_temp", ignore_errors=True)

    n = DiskBatchWriterOperator(dataset="_temp/", format="zstd")
    n.execute(data={"this": "is", "a": "record"}, context={})
    n.finalize({})

    assert os.path.exists("_temp")

    shutil.rmtree("_temp", ignore_errors=True)


if __name__ == "__main__":  # pragma: no cover
    test_save_to_disk_operator()

    print("okay")
