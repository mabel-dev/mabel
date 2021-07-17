"""
LRU Index

Implements an arbitrary length LRU Index, this is not a LRU cache, this is the 'brains'
of a deduplication engine - where the most recently seen 'size' items are remembered
and older items are ejected.

Some implementation decisions are to optimize for speed and memory - when deduplicating
millions of records, speed is important.

#nodoc - don't add to the documentation wiki
"""
from typing import Any, List


class LruIndex(object):

    __slots__ = ("hash_list", "size")

    def __init__(self, size: int = 1000):
        self.hash_list: dict = {}
        self.size = size + 1

    def test(self, item: Any):
        # this offers a minor performance improvement
        hash_list = self.hash_list
        # pop() will remove the item if it's in the dict
        # we return a default of False
        if hash_list.pop(item, False):
            hash_list[item] = True
            return True
        # add the item to the top of the dict
        hash_list[item] = True
        if len(hash_list) == self.size:
            # we want to remove the first item in the dict
            # we could convert to a list, but then we need
            # to create a list, this is much faster and
            # uses less memory
            # deepcode ignore unguarded~next~call: will not error
            hash_list.pop(next(iter(hash_list)))
        return False

    def __call__(self, item: Any):
        return self.test(item)
