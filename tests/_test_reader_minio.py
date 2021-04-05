import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from mabel.adapters.minio import MinIoReader
from mabel.data import Reader
try:
    from rich import traceback
    traceback.install()
except ImportError:
    pass


def test_minio():

    reader = Reader(
            end_point=os.getenv('MINIO_END_POINT'),
            access_key=os.getenv('MINIO_ACCESS_KEY'),
            secret_key=os.getenv('MINIO_SECRET_KEY'),
            secure=False,
            dataset='SNAPSHOTS/NVD/NVD_CVE_LIST',
            inner_reader=MinIoReader,
            step_back_days=30
    )

    for i, item in enumerate(reader):
        pass

    print(i, item)


if __name__ == "__main__":
    test_minio()

    print('okay')
