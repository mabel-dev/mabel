import shutil
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))
from mabel.adapters.google import GoogleCloudStorageWriter, GoogleCloudStorageReader
from mabel import Reader
from mabel.data import BatchWriter
from google.auth.credentials import AnonymousCredentials
from google.cloud import storage
from rich import traceback

traceback.install()

BUCKET_NAME = "PYTEST"


def set_up():

    shutil.rmtree(".cloudstorage", ignore_errors=True)

    os.environ["STORAGE_EMULATOR_HOST"] = "http://localhost:9090"

    client = storage.Client(
        credentials=AnonymousCredentials()
    )
    bucket = client.bucket(BUCKET_NAME)
    try:
        bucket.delete(force=True)
    except:  # pragma: no cover
        pass
    bucket = client.create_bucket(BUCKET_NAME)


def test_gcs_binary():

    # set up
    set_up()

    w = BatchWriter(
        inner_writer=GoogleCloudStorageWriter,
        project="testing",
        blob_size=1024,
        dataset=f"{BUCKET_NAME}/test/gcs/dataset/binary",
    )
    for i in range(200):
        w.append({"index": i + 300})
    w.finalize()

    # read the files we've just written, we should be able to
    # read over both paritions.
    r = Reader(
        inner_reader=GoogleCloudStorageReader,
        dataset=f"{BUCKET_NAME}/test/gcs/dataset/binary"
    )

    assert r.count() == 200, r.count()


def test_gcs_text():

    # set up
    set_up()

    w = BatchWriter(
        inner_writer=GoogleCloudStorageWriter,
        blob_size=1024,
        format="jsonl",
        dataset=f"{BUCKET_NAME}/test/gcs/dataset/text",
    )
    for i in range(250):
        w.append({"index": i + 300})
    w.finalize()

    # read the files we've just written, we should be able to
    # read over both paritions.
    r = Reader(
        inner_reader=GoogleCloudStorageReader,
        dataset=f"{BUCKET_NAME}/test/gcs/dataset/text",
    )

    assert r.count() == 250, r


if __name__ == "__main__":  # pragma: no cover
    test_gcs_binary()
    test_gcs_text()

    print("okay")
