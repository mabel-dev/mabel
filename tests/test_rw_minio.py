import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from mabel.adapters.minio import MinIoWriter, MinIoReader
from mabel.operators.minio import MinIoBatchWriterOperator
from mabel.data import BatchWriter, Reader
try:
    from rich import traceback
    traceback.install()
except ImportError:
    pass

VAMPIRIC_COUNCIL = [
    {'player':'Tilda Swinton','from':'Only Lovers Left Alive'},
    {'player':'Evan Rachel Wood','from':'True Blood'},
    {'player':'Danny Trejo','from':'From Duck Til Dawn'},
    {'player':'Paul Reubens','from':'Buffy the Vampire Slayer'},
    {'player':'Wesley Snipes','from':'Blade'}
]

def test_using_batch_writer():

    errored = False
    try:
        w = BatchWriter(
                inner_writer=MinIoWriter,
                end_point=os.getenv('MINIO_END_POINT'),
                access_key=os.getenv('MINIO_ACCESS_KEY'),
                secret_key=os.getenv('MINIO_SECRET_KEY'),
                secure=False,
                dataset='TEST/test_writer')

        for member in VAMPIRIC_COUNCIL:
            w.append(member)
        w.finalize()
    except:
        errored = True

    assert not errored


def test_using_operator():

    w = MinIoBatchWriterOperator(
            end_point=os.getenv('MINIO_END_POINT'),
            access_key=os.getenv('MINIO_ACCESS_KEY'),
            secret_key=os.getenv('MINIO_SECRET_KEY'),
            secure=False,
            dataset='TEST/test_operator')

    for member in VAMPIRIC_COUNCIL:
        w.execute(member)
    w.finalize()

    reader = Reader(
            end_point=os.getenv('MINIO_END_POINT'),
            access_key=os.getenv('MINIO_ACCESS_KEY'),
            secret_key=os.getenv('MINIO_SECRET_KEY'),
            secure=False,
            dataset='TEST/test_operator',
            inner_reader=MinIoReader
    )

    for i, item in enumerate(reader):
        pass

    assert (i+1) == len(VAMPIRIC_COUNCIL), i
    assert item == VAMPIRIC_COUNCIL[i]

    
if __name__ == "__main__":
    test_using_batch_writer()
    test_using_operator()

    print("okay")