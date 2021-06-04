import shutil
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.adapters.google import GoogleCloudStorageWriter, GoogleCloudStorageReader
from mabel.data import BatchWriter
from mabel.data import Reader
from google.auth.credentials import AnonymousCredentials
from google.cloud import storage
from rich import traceback

traceback.install()

BUCKET_NAME = "PYTEST"


def set_up():

    shutil.rmtree(".cloudstorage", ignore_errors=True)

    os.environ["STORAGE_EMULATOR_HOST"] = "http://localhost:9090"

    client = storage.Client(
        credentials=AnonymousCredentials(),
        project="testing",
    )
    bucket = client.bucket(BUCKET_NAME)
    try:
        bucket.delete()
    except:  # pragma: no cover
        pass
    bucket = client.create_bucket(bucket)


def test_gcs():

    try:
        # set up
        set_up()

        w = BatchWriter(
            inner_writer=GoogleCloudStorageWriter,
            project="testing",
            blob_size=1024,
            dataset=f"{BUCKET_NAME}/test/gcs/dataset",
        )
        for i in range(200):
            w.append({"index": i + 300})
        w.finalize()

        # read the files we've just written, we should be able to
        # read over both paritions.
        r = Reader(
            inner_reader=GoogleCloudStorageReader,
            project="testing",
            dataset=f"{BUCKET_NAME}/test/gcs/dataset",
        )
        l = list(r)

        assert len(l) == 200, len(l)
    except Exception as e:  # pragma: no cover
        raise e


if __name__ == "__main__":  # pragma: no cover
    test_gcs()

    print("okay")
