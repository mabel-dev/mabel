import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))
from mabel.adapters.google import GoogleCloudStorageWriter, GoogleCloudStorageReader
from mabel.data import BatchWriter
from mabel.data import Reader
from google.auth.credentials import AnonymousCredentials
from google.cloud import storage
from rich import traceback

traceback.install()

BUCKET_NAME = "pytest"


def set_up():

    os.environ["STORAGE_EMULATOR_HOST"] = "http://localhost:9090"

    client = storage.Client(credentials=AnonymousCredentials())
    bucket = client.bucket(BUCKET_NAME)
    try:
        bucket.delete(force=True)
    except:  # pragma: no cover
        pass
    bucket = client.create_bucket(BUCKET_NAME)


def test_gcs_parquet():

    try:
        # set up the stub
        set_up()

        w = BatchWriter(
            inner_writer=GoogleCloudStorageWriter,
            format="parquet",
            dataset=f"{BUCKET_NAME}/test/gcs/dataset",
        )
        for i in range(100):
            w.append({"$$": i * 300})
        w.finalize()

        # read the files we've just written, we should be able to
        # read over both paritions.
        r = Reader(
            inner_reader=GoogleCloudStorageReader,
            dataset=f"{BUCKET_NAME}/test/gcs/dataset",
        )
        l = list(r)
        assert isinstance(l[0], dict)
        assert len(l) == 100, len(l)
    except Exception as e:  # pragma: no cover
        raise e


if __name__ == "__main__":  # pragma: no cover
    test_gcs_parquet()

    print("okay")
