"""
StorageClassCompressedMemory is a helper class for persisting DictSets locally, it is
the backend for the COMPRESSED_MEMORY variation of the STORAGE CLASSES.

This is particularly useful when local storage is slow and the data is too big for
memory, or you are doing a lot of operations on the data.

If you're doing few operations on the the data and it is easily recreated or local
storage is fast, this isn't a good option.
"""

import gc
from typing import List
from typing import Set
from typing import Tuple
from typing import Union

import lz4.frame

from mabel.data.internals.storage_classes import BaseStorageClass

BATCH_SIZE = 250


class StorageClassCompressedMemory(BaseStorageClass):
    def __init__(self, iterable):
        self.compressor = lz4.frame
        self.batches: List[bytearray] = []
        self.length = 0
        self.iterator = None
        self.decompressor = lz4.frame

        batch = []
        for item in iterable:
            batch.append(item)
            self.length += 1
            if len(batch) >= BATCH_SIZE:
                compressed_batch = self.compressor.compress(self.dump_json(batch))
                self.batches.append(bytearray(compressed_batch))
                batch.clear()

        if batch:
            compressed_batch = self.compressor.compress(self.dump_json(batch))
            self.batches.append(bytearray(compressed_batch))

        # Force garbage collection (though it may not provide significant benefits)
        del batch
        gc.collect()

    def _inner_reader(self, *locations: Union[int, Tuple[int], List[int], Set[int]]):
        if locations:
            ordered_location = sorted(locations)
            batch_number = -1
            batch = []
            for i in ordered_location:
                requested_batch = i // BATCH_SIZE
                if requested_batch != batch_number:
                    batch = self.parse_json(
                        self.decompressor.decompress(self.batches[requested_batch])
                    )
                yield batch[i % BATCH_SIZE]

        else:
            for batch in self.batches:
                records = self.parse_json(self.decompressor.decompress(batch))
                for record in records:
                    if record:
                        yield record

    def __iter__(self):
        self.iterator = iter(self._inner_reader())
        return self.iterator

    def __next__(self):
        if not self.iterator:
            self.iterator = iter(self._inner_reader())
        return next(self.iterator)

    def __len__(self):
        return self.length
