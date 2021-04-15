
import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from mabel.index.bloom_filter import BloomFilter
try:
    from rich import traceback
    traceback.install()
except ImportError:
    pass


def test_bf():

    size = 500
    fp = 0.05

    bf = BloomFilter(size, fp)
    for i in range(size):
        bf.add(F"{i}")

    for i in range(size):
        assert F"{i}" in bf

    collector = [] 
    for i in range(size):
        if F"{i + size}" in bf:
            collector.append(i + size)

    assert len(collector) <= (size * fp * 2)


if __name__ == "__main__":
    test_bf()

    print('okay')
