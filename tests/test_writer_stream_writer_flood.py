import os
import sys
import time

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.adapters.null import NullWriter
from mabel.data import StreamWriter

from mabel.logging import get_logger

logger = get_logger()


def test_writer_flood():
    logger.setLevel(5)
    w = StreamWriter(
        dataset="path/{identity}/path",
        inner_writer=NullWriter,
        idle_timeout_seconds=1,
        schema=["identity", "cycle"],
    )

    for i in range(1000):
        record = {"identity": str(os.urandom(1)), "cycle": i}
        time.sleep(0.001)
        w.append(record)

    logger.info("FINISHED WRITING")

    print(len(w.writer_pool.writers))

    w.finalize()


if __name__ == "__main__":  # pragma: no cover
    from tests.helpers.runner import run_tests

    run_tests()
