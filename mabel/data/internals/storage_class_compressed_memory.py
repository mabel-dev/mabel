"""
StorageClassCompressedMemory is a helper class for persisting DictSets locally, it is
the backend for the COMPRESSED_MEMORY variation of the STORAGE CLASSES.

This is particularly useful when local storage is slow and the data is too big for
memory, or you are doing a lot of operations on the data.

If you're doing few operations on the the data and it is easily recreated or local
storage is fast, this isn't a good option.
"""
import orjson
import lz4.frame
from itertools import zip_longest

BATCH_SIZE = 500


class StorageClassCompressedMemory(object):
    def __init__(self, iterable):
        compressor = lz4.frame

        self.batches = []
        self.length = 0

        for batch in zip_longest(*[iterable] * BATCH_SIZE):
            self.length += len(batch)
            self.batches.append(compressor.compress(orjson.dumps(batch)))

        # the last batch fills with Nones
        self.length -= batch.count(None)

    def _inner_reader(self, *locations):
        decompressor = lz4.frame

        if locations:
            if not isinstance(locations, (tuple, list, set)):
                locations = [locations]
            ordered_location = sorted(locations)
            batch_number = -1
            batch = []
            for i in ordered_location:
                requested_batch = i % BATCH_SIZE
                if requested_batch != batch_number:
                    batch = orjson.loads(
                        decompressor.decompress(self.batches[requested_batch])
                    )
                yield batch[i - i % BATCH_SIZE]

        else:
            for batch in self.batches:
                records = orjson.loads(decompressor.decompress(batch))
                for record in records:
                    if record:
                        yield record

    def __iter__(self):
        return self._inner_reader()

    def __next__(self):
        return next(self)

    def __len__(self):
        return self.length
