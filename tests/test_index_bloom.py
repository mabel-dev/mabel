
import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from mabel.index.bloom_filter import BloomFilter
from rich import traceback

traceback.install()


def test_bf():

    size = 500
    fp = 0.05

    bf = BloomFilter(size, fp)
    for i in range(size):
        bf.add(F"{i}")

    # test all of the added items are found
    for i in range(size):
        assert F"{i}" in bf, F'test_bf - find {i}'

    # test that some items are not found
    collector = [] 
    for i in range(size):
        if F"{i + size}" in bf:
            collector.append(i + size)

    assert len(collector) <= (size * fp * 2), F'test_bf {len(collector)} {(size * fp * 2)}'

def test_bf_persistence():

    b = BloomFilter(1000,0.05)
    for i in range(5):
        b.add(F"{i}")

    previous_hash_count = b.hash_count
    previous_filter_size = b.filter_size

    BloomFilter.write_bloom_filter(b, '_temp/filter.bloom') 
    bf = BloomFilter.read_bloom_filter('_temp/filter.bloom')

    assert bf.hash_count == previous_hash_count
    assert bf.filter_size == previous_filter_size

    for i in range(5):
        assert F"{i}" in bf

if __name__ == "__main__":  # pragma: no cover
    test_bf()
    test_bf_persistence()

    print('okay')
