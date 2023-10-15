import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.data.writers.internals.writer_pool import WriterPool
from mabel.adapters.null import NullWriter
from rich import traceback

traceback.install()


def test_writer_pool_lru():
    pool = WriterPool(3, inner_writer=NullWriter, schema=["record"])
    pool.get_writer("beatle/john.txt")
    pool.get_writer("beatle/paul.txt")
    pool.get_writer("beatle/george.txt")

    # with three items, nothing should be on the eviction list
    assert len(pool.nominate_writers_to_evict()) == 0
    pool.get_writer("beatle/ringo.txt")

    # we've added four items but not evicted any, should have four items
    assert len(pool.writers) == 4
    # four is one more than three
    to_evict = pool.nominate_writers_to_evict()
    assert len(to_evict) == 1
    # john is the least recently used
    assert to_evict[0] == "beatle/john.txt"

    pool.get_writer("beatle/john.txt")

    # we accessed john, paul us now the LRU
    to_evict = pool.nominate_writers_to_evict()
    assert len(to_evict) == 1
    assert to_evict[0] == "beatle/paul.txt"


def test_writer_add_and_remove():
    pool = WriterPool(3, inner_writer=NullWriter, schema=["record"])
    pool.get_writer("beatle/john.txt")
    pool.get_writer("beatle/paul.txt")

    # we should have two writers in the pool
    assert len(pool.writers) == 2

    pool.remove_writer("beatle/john.txt")

    # we removed one so we should have only paul in the pool
    assert len(pool.writers) == 1
    assert pool.writers[0].get("identity") == "beatle/paul.txt"

    pool.get_writer("beatle/george.txt")

    # george should be in the pool with paul
    assert len(pool.writers) == 2


if __name__ == "__main__":  # pragma: no cover
    from tests.helpers.runner import run_tests

    run_tests()
