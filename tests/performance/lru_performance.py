import time
import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '../..'))
from mabel.index.lru_index import LruIndex
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

def lru_performance():

    lru = LruIndex(size=1000)

    values = []
    for i in range(5000):
        values.append(entropy.random_choice(word_list))

    start = time.time_ns()

    for i in range(3000):
        for val in values:
            lru.test(val)

    print((time.time_ns() - start) / 1e9)


if __name__ == "__main__":
    lru_performance()

    print('okay')






