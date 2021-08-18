import shutil
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.adapters.azure import AzureBlobStorageReader, AzureBlobStorageWriter
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError
from mabel.data import BatchWriter
from mabel.data import Reader
from rich import traceback

traceback.install()

BUCKET_NAME = "pytest"


def set_up():
    # Create a container for Azurite for the first run
    blob_service_client = BlobServiceClient.from_connection_string(os.environ.get("AZURE_STORAGE_CONNECTION_STRING"))
    
    try:
        blob_service_client.delete_container(BUCKET_NAME)
    except:
        pass
    
    try:
        blob_service_client.create_container(BUCKET_NAME)
    except ResourceExistsError:
        pass



def test_azure():

    try:
        # set up
        set_up()

        w = BatchWriter(
            inner_writer=AzureBlobStorageWriter,
            blob_size=1024,
            dataset=f"{BUCKET_NAME}/test/azure/dataset",
        )
        for i in range(200):
            w.append({"index": i + 300})
        w.finalize()

        # read the files we've just written, we should be able to
        # read over both paritions.
        r = Reader(
            inner_reader=AzureBlobStorageReader,
            dataset=f"{BUCKET_NAME}/test/azure/dataset",
        )
        l = list(r)

        assert len(l) == 200, len(l)
    except Exception as e:  # pragma: no cover
        raise e


if __name__ == "__main__":  # pragma: no cover
    test_azure()

    print("okay")
