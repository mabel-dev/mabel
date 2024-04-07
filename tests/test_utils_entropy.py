"""
Note that this is testing randomness functions, failure is inevitable on a
long enough series of tests (think billions)
"""

import sys
import os

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.utils import entropy
from rich import traceback

traceback.install()


def test_random_string():
    """test the string is the right length"""
    rnd_str = entropy.random_string(length=32)
    assert len(rnd_str) == 32


def test_random_int():
    # this test will eventually fail
    rnd_1 = entropy.random_int()
    rnd_2 = entropy.random_int()

    assert not rnd_1 == rnd_2
    assert isinstance(rnd_1, int)


def test_random_range():
    vals = []
    for i in range(5000):
        vals.append(entropy.random_range(10, 20))

    assert min(vals) == 10, "test lower extreme of range"
    assert max(vals) == 20, "test upper extreme of range"


def test_bytes_to_int():
    b1 = [0] * 8
    assert entropy.bytes_to_int(b1) == 0

    b2 = [255] * 4
    assert entropy.bytes_to_int(b2) == (2**32 - 1)

    b3 = [255] * 8
    assert entropy.bytes_to_int(b3) == (2**64 - 1)


def test_random_choice():
    options = ["one", "two", "three"]

    for i in range(100):
        choice = entropy.random_choice(options)
        assert choice in options


if __name__ == "__main__":  # pragma: no cover
    from tests.helpers.runner import run_tests

    run_tests()
