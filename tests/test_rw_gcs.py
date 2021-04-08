import random
import string
import shutil
import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from mabel.adapters.google import GoogleCloudStorageWriter, GoogleCloudStorageReader
from mabel.data import Reader, BatchWriter
from google.auth.credentials import AnonymousCredentials
from google.cloud import storage
from gcp_storage_emulator.server import create_server
try:
    from rich import traceback
    traceback.install()
except ImportError:
    pass

# randomize the bucket name to avoid collisions on reruns
CHARACTERS = string.ascii_lowercase + string.digits
BUCKET_NAME = ''.join(random.choice(CHARACTERS) for i in range(8))

def set_up():

    shutil.rmtree('.cloudstorage', ignore_errors=True)
    
    server = create_server(host="localhost", port=9090, in_memory=False)
    server.start()
    os.environ['STORAGE_EMULATOR_HOST'] = 'http://localhost:9090'

    client = storage.Client(
        credentials=AnonymousCredentials(),
        project="testing",
    )
    bucket = client.bucket(BUCKET_NAME)
    try:
        bucket = client.create_bucket(bucket)
    except:
        pass

    return server


def test_gcs():

    try:
        # set up the stub
        server = set_up()

        w = BatchWriter(
                inner_writer=GoogleCloudStorageWriter,
                project='testing',
                blob_size=1024,
                dataset=F'{BUCKET_NAME}/test/gcs/dataset')
        for i in range(200):
            w.append({"index":i+300})
        w.finalize()

        # read the files we've just written, we should be able to
        # read over both paritions.
        r = Reader(
            inner_reader=GoogleCloudStorageReader,
            project='testing',
            dataset=F'{BUCKET_NAME}/test/gcs/dataset'
        )
        l = list(r)

        assert len(l) == 200, len(l)
    except Exception as e:
        raise e
    finally:
        server.stop()


if __name__ == "__main__":
    test_gcs()

    print('okay')