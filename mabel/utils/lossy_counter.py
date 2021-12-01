"""
Find the most frequent item in an infinite (i.e. larger than will fit in memory)
dataset.

Based on LossyCounter by Manku and Motwani
"""
from typing import Dict, List, Any

TRACKED_ITEM_COUNT = 100


class LossyCounter:
    def __init__(self, items: int = TRACKED_ITEM_COUNT):
        self.max_items = items
        self.tracked_items: Dict[Any, int] = {}
        self.bucket: List[Any] = []

    def add(self, item):

        # divide the incoming data stream into buckets
        if len(self.bucket) < self.max_items:
            self.bucket.append(str(item))

        else:
            # add the last item to the bucket before we empty it
            self.bucket.append(str(item))
            self.empty_bucket()

    def empty_bucket(self):

        for i in self.bucket:
            # if it exists on the list already, increment the frequency by one
            if i in self.tracked_items:
                self.tracked_items[i] += 1
            # if it's new and if we have room in the list for new items, add it
            elif len(self.tracked_items) <= self.max_items:
                self.tracked_items[i] = 1
            # if it's new and the list is full, remove the least frequent item
            else:
                key_to_remove = min(self.tracked_items, key=self.tracked_items.get)
                self.tracked_items.pop(key_to_remove)
                self.tracked_items[i] = 1

        # after each bucket, decrement the counters by 1
        for k, v in self.tracked_items.items():
            self.tracked_items[k] = v - 1

        self.bucket = []

    def most_frequent(self):
        self.empty_bucket()
        if self.tracked_items:
            return max(self.tracked_items, key=self.tracked_items.get)
        return None
