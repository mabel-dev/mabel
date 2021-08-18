"""
StorageClassCompressedMemory is a helper class for persisting DictSets locally, it is
the backend for the COMPRESSED_MEMORY variation of the STORAGE CLASSES.

This is particularly useful when local storage is slow and the data is too big for
memory, or you are doing a lot of operations on the data.

If you're doing few operations on the the data and it is easily recreated or local
storage is fast, this isn't a good option
"""
import orjson
import lz4.frame

BATCH_SIZE = 1000

class Dummy():
    @staticmethod
    def compress(var):
        return var
    @staticmethod
    def decompress(var):
        return var

class StorageClassCompressedMemory(object):

    def __init__(self, iterator):
        compressor = lz4.frame
#        compressor = Dummy()

        self.batches = []

        self.length = -1
        batch = []
        for self.length, row in enumerate(iterator):
            batch.append(orjson.dumps(row))
            if self.length and self.length % BATCH_SIZE:
                self.batches.append(compressor.compress(b'[' + b','.join(batch) + b']'))
                batch = []
        if batch:
            self.batches.append(compressor.compress(b'[' + b','.join(batch) + b']'))
        self.length += 1

    def _inner_reader(self, *locations):
        decompressor = lz4.frame
#        decompressor = Dummy()

        if locations:
            if not isinstance(locations, (tuple, list, set)):
                locations = [locations]
            ordered_location = sorted(locations)
            batch_number = -1
            batch = []
            for i in locations:
                requested_batch = i % BATCH_SIZE
                if requested_batch != batch_number:
                    batch = orjson.loads(decompressor.decompress(self.batches[requested_batch]))
                yield batch[i - i % BATCH_SIZE]

        else:
            for batch in self.batches:
                records = orjson.loads(decompressor.decompress(batch))
                for record in records:
                    yield record

    def __iter__(self):
        return self._inner_reader()

    def __next__(self):
        return next(self)

    def __len__(self):
        return self.length
