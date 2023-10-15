import time
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.adapters.null import NullWriter
from mabel.data import StreamWriter


EXPECTED_RESULTS = {1: 1, 2: 2, 3: 3, 4: 1}


def test_writer_timeout():
    # none of these should do anything
    w = StreamWriter(
        dataset="bucket/path/file.extension",
        inner_writer=NullWriter,
        idle_timeout_seconds=2,
        schema=["data"],
    )

    # with a two second wait, we should timeout and start writing to a new partition
    # after the third write, which has a three second wait (nearly)
    for i in range(1, 5):
        l = w.append({"data": "data"})
        # print(f"cycle {i} - {l} ? {EXPECTED_RESULTS[i]}")
        assert l == EXPECTED_RESULTS[i], f"cycle {i} - {l} != {EXPECTED_RESULTS[i]}"
        if i < len(EXPECTED_RESULTS):
            time.sleep(i * 0.95)


if __name__ == "__main__":  # pragma: no cover
    from tests.helpers.runner import run_tests

    run_tests()
