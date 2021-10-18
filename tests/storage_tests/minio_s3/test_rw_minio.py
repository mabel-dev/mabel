import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))
from mabel.adapters.minio import MinIoWriter, MinIoReader
from mabel.operators.minio import MinIoBatchWriterOperator
from mabel.data import BatchWriter
from mabel.data import Reader
from rich import traceback

traceback.install()


BUCKET_NAME = "test"

VAMPIRIC_COUNCIL = [
    {"player": "Tilda Swinton", "from": "Only Lovers Left Alive"},
    {"player": "Evan Rachel Wood", "from": "True Blood"},
    {"player": "Danny Trejo", "from": "From Dusk Til Dawn"},
    {"player": "Paul Reubens", "from": "Buffy the Vampire Slayer"},
    {"player": "Wesley Snipes", "from": "Blade"},
    {"player": "Brad Pitt", "from": "Interview with a Vampire"},
    {"player": "Kiefer Sutherland", "from": "The Lost Boys"},
    {"player": "Jemaine Clement", "from": "What We Do in the Shadows"},
]


def _create_bucket():
    from minio import Minio

    end_point = os.getenv("MINIO_END_POINT")
    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")

    client = Minio(end_point, access_key, secret_key, secure=False)
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)


def test_using_batch_writer():

    errored = False
    #try:
    if True:
        _create_bucket()
        w = BatchWriter(
            inner_writer=MinIoWriter,
            end_point=os.getenv("MINIO_END_POINT"),
            access_key=os.getenv("MINIO_ACCESS_KEY"),
            secret_key=os.getenv("MINIO_SECRET_KEY"),
            secure=False,
            dataset=f"{BUCKET_NAME}/test_writer",
        )

        for member in VAMPIRIC_COUNCIL:
            w.append(member)
        w.finalize()
    #except Exception as a:
    #    print(a)
    #    errored = True

    assert not errored


def test_using_operator():

    _create_bucket()

    w = MinIoBatchWriterOperator(
        end_point=os.getenv("MINIO_END_POINT"),
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
        secure=False,
        dataset=f"{BUCKET_NAME}/test_operator",
    )

    for member in VAMPIRIC_COUNCIL:
        w.execute(member)
    w.finalize()

    reader = Reader(
        end_point=os.getenv("MINIO_END_POINT"),
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
        secure=False,
        dataset=f"{BUCKET_NAME}/test_operator",
        inner_reader=MinIoReader,
    )

    for i, item in enumerate(reader):
        pass

    assert (i + 1) == len(VAMPIRIC_COUNCIL), i
    assert item == VAMPIRIC_COUNCIL[i]


if __name__ == "__main__":  # pragma: no cover
    test_using_batch_writer()
    test_using_operator()

    print("okay")
