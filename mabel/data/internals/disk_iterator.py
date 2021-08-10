import orjson
import atexit
import os
from tempfile import NamedTemporaryFile

BUFFER_SIZE = 16 * 1024 * 1024  # 16Mb


class DiskIterator(object):
    """
    This provides the reader for the DISK variation of STORAGE.
    """

    def __init__(self, iterator):
        self.inner_reader = None
        self.length = -1

        self.file = NamedTemporaryFile(prefix="mabel-dictset").name
        atexit.register(os.remove, self.file)
        print(self.file)

        with open(self.file, "wb", buffering=BUFFER_SIZE) as f:
            for self.length, row in enumerate(iterator):
                f.write(orjson.dumps(row) + b"\n")
        f.close()

    def _read_file(self):
        """
        Reading files in chunks improves performance.

        We've chosen a conservative 8Mb cache as one of the reasons to use
        DISK to persist the dataset is because you have limited memory.
        """
        with open(self.file, "r", encoding="utf8") as f:
            carry_forward = ""
            chunk = "INITIALIZED"
            while len(chunk) > 0:
                chunk = f.read(BUFFER_SIZE)
                augmented_chunk = carry_forward + chunk
                lines = augmented_chunk.splitlines()
                carry_forward = lines.pop()
                yield from lines
            if carry_forward:
                yield carry_forward

    def _inner_reader(self, *locations):
        if locations:
            max_location = max(locations)
            min_location = min(locations)

            reader = self._read_file()

            for i in range(min_location):
                next(reader)

            for i, line in enumerate(reader, min_location):
                if i in locations:
                    yield orjson.loads(line)
                    if i == max_location:
                        return
        else:
            for line in self._read_file():
                yield orjson.loads(line)

    def __iter__(self):
        self.inner_reader = self._inner_reader()
        return self

    def __next__(self):
        record = next(self.inner_reader)
        if record:
            return record

    def __len__(self):
        return self.length
