import time
import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '../..'))
from mabel.index.bloom_filter import BloomFilter
from mabel.data.formats.dictset import drop_duplicates
from mabel.utils import entropy
try:
    from rich import traceback
    traceback.install()
except ImportError:
    pass

word_list = []
with open('tests/data/word_list.txt') as words:
    for word in words:
        word_list.append(word.rstrip())

def dedupe():
    values = []
    for i in range(100000):
        values.append(entropy.random_choice(word_list))
    start = time.time_ns()
    for i in range(20):
        [v for v in drop_duplicates(values,2000)]
    print((time.time_ns() - start) / 1e9)


def drop(dictset):
    bf = BloomFilter(10000)
    for record in dictset:
        if record in bf:
            continue
        bf.add(record)
        yield record


def filt():
    values = []
    for i in range(100000):
        values.append(entropy.random_choice(word_list))
    start = time.time_ns()
    for i in range(20):
        a = [v for v in drop(values)]
    print((time.time_ns() - start) / 1e9)
    print(len(a))


if __name__ == "__main__":
    dedupe()
    filt()

    print('okay')






