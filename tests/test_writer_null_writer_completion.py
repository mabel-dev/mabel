import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.adapters.null import NullWriter
from mabel.data import BatchWriter
from rich import traceback

traceback.install()

ROBOTS = [
    {"name": "K-2SO"},
    {"name": "Jonny-5"},
    {"name": "Astro Boy"},
    {"name": "Vision"},
    {"name": "Ava"},
    {"name": "Baymax"},
    {"name": "WALL-E"},
    {"name": "T-800"},
]


def test_null_writer():
    # none of these should do anything

    w = BatchWriter(inner_writer=NullWriter, dataset="nowhere")
    for bot in ROBOTS:
        w.append(bot)

    try:
        # pass
        # sys.exit()
        raise Exception("I'm afraid I can't do that")
    except:
        pass
    finally:
        assert w.finalize()


if __name__ == "__main__":  # pragma: no cover
    test_null_writer()

    print("okay")
