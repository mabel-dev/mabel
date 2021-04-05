
import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from mabel.utils.lru_index import LruIndex
try:
    from rich import traceback
    traceback.install()
except ImportError:
    pass

item_1 = 'one'
item_2 = 'two'
item_3 = 'three'
item_4 = 'four'
item_5 = 'five'

def test_lru():

    lru = LruIndex(size=3)

    assert not lru(item_1), item_1  # first item shouldn't be on there
    assert not lru(item_2), item_2  # different second item, not there
    assert lru(item_2), item_2      # repeat of second item, is there
    assert not lru(item_3), item_3  # new third item, not there
    assert lru(item_1), item_1      # the first item should be there
    assert not lru(item_4), item_4  # new forth item, not there, item_2 ejected

    assert lru(item_1), item_1      # test the expected items are present
    assert lru(item_3), item_3
    assert lru(item_4), item_4

    assert not lru(item_5), item_5  # add a new item, eject item_1
    assert lru(item_5), item_5      # test the expected items are present
    assert lru(item_3), item_3
    assert lru(item_4), item_4

    assert not lru(item_1), item_1  # confirm item_1 was ejected above


if __name__ == "__main__":
    test_lru()

    print('okay')
