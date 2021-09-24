import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))
from mabel.adapters.minio import MinIoReader
from mabel.data import Reader
from rich import traceback

traceback.install()


# this is a public S3 bucket
BUCKET_NAME = "mabel.labs"


# this data is used for the labs
def test_reading_aws_using_minio():

    reader = Reader(
        end_point="s3.eu-west-2.amazonaws.com",
        access_key=None,
        secret_key=None,
        secure=True,
        dataset=f"{BUCKET_NAME}/538/StarWars",
        inner_reader=MinIoReader,
        raw_path=True,
    )

    data = list(reader)
    assert len(data) == 1186, len(data)


if __name__ == "__main__":  # pragma: no cover
    test_reading_aws_using_minio()

    print("okay")
