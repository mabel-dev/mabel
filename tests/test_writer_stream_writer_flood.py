import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.adapters.null import NullWriter
from mabel.data import StreamWriter
from rich import traceback

traceback.install()


def test_writer_flood():

    w = StreamWriter(
        dataset="path/{identity}/path",
        inner_writer=NullWriter,
        idle_timeout_seconds=1,
    )

    for i in range(100000):

        record = { "identity": str(os.urandom(1)), "cycle": i }
        w.append(record)

    print(len(w.writer_pool.writers))

    w.finalize()


if __name__ == "__main__":  # pragma: no cover
    test_writer_flood()
    print("okay")
