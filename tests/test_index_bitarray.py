import datetime
import sys
import os
import pytest
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from mabel.index.bitarray import bitarray
from rich import traceback

traceback.install()


def test_index_bitarray():

    EXPECTED_RESULTS = [
        "bitarray('11111111111111111111111111111111')",
        "bitarray('01010101010101010101010101010101')",
        "bitarray('11000111000111000111000111000111')",
        "bitarray('01001111100101001111100101001111')",
        "bitarray('11001011101101011111000100001101')"
    ]

    SIZE = 32
    bits = bitarray(SIZE)

    bits.setall(1)
    assert repr(bits) == "bitarray('11111111111111111111111111111111')", "set one way"
    bits.setall(0)
    assert repr(bits) == "bitarray('00000000000000000000000000000000')", "set the other way"
    bits.setall(1)
    assert repr(bits) == "bitarray('11111111111111111111111111111111')", "set back the first way"
    bits.setall(1)
    assert repr(bits) == "bitarray('11111111111111111111111111111111')", "set the same way"
    bits.setall(0)
    assert repr(bits) == "bitarray('00000000000000000000000000000000')", "swap"
    bits.setall(0)
    assert repr(bits) == "bitarray('00000000000000000000000000000000')", "set the same way the other way"

    for step in range(5):
        for i in range(0, SIZE, step+1):
            bits[i] = abs(bits[i] - 1)
        print(bits)
        assert repr(bits) == EXPECTED_RESULTS[step], repr(bits)

if __name__ == "__main__":  # pragma: no cover
    test_index_bitarray()

    print('okay')
