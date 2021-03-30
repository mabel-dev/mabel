import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from mabel.adapters.minio.minio_writer import MinIoWriter
from mabel.operators.minio import SaveSnapshotToMinIoOperator
from mabel.data import BatchWriter
try:
    from rich import traceback
    traceback.install()
except ImportError:
    pass

def test_using_batch_writer():

    w = BatchWriter(
            inner_writer=MinIoWriter,
            end_point=os.getenv('MINIO_END_POINT'),
            access_key=os.getenv('MINIO_ACCESS_KEY'),
            secret_key=os.getenv('MINIO_SECRET_KEY'),
            secure=False,
            dataset='TWITTER/test')

    import time

    start = time.time_ns()

    for i in range(100):
        w.append({"tv":i+100})
    w.finalize()

    print('okay', (start - time.time_ns())/1e9)


def test_using_operator():

    w = SaveSnapshotToMinIoOperator(
            end_point=os.getenv('MINIO_END_POINT'),
            access_key=os.getenv('MINIO_ACCESS_KEY'),
            secret_key=os.getenv('MINIO_SECRET_KEY'),
            secure=False,
            dataset='TWITTER/test')


if __name__ == "__main__":
    test_using_operator()
