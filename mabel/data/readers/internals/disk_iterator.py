import glob
import orjson


class DiskIterator:
    """
    This provides the reader for the DISK variation of STORAGE.
    """

    def __init__(self, folder):
        self.folder = folder
        self.inner_reader = None
        self.files = []

    def _read_file_in_chunks(self, filename, chunk_size=8 * 1024 * 1024):
        """
        Reading files in chunks improves performance.

        We've chosen a conservative 8Mb cache as one of the reasons to use
        DISK to persist the dataset is because you have limited memory.
        """
        with open(filename, "r", encoding="utf8") as f:
            carry_forward = ""
            chunk = "INITIALIZED"
            while len(chunk) > 0:
                chunk = f.read(chunk_size)
                augmented_chunk = carry_forward + chunk
                lines = augmented_chunk.splitlines()
                carry_forward = lines.pop()
                yield from lines
            if carry_forward:
                yield carry_forward

    def _inner_reader(self):
        if not self.files:
            self.files = glob.glob(self.folder + "/**.jsonl")
        for file in self.files:
            for line in self._read_file_in_chunks(file):
                yield orjson.loads(line)

    def __iter__(self):
        self.inner_reader = self._inner_reader()
        return self

    def __next__(self):
        record = next(self.inner_reader)
        if record:
            return record

    def __getitem__(self, items):
        collected_items = []
        if not isinstance(items, (list, set, tuple)):
            items = set([items])
        max_item = max(items)
        for i, r in enumerate(self):
            if i in items:
                collected_items.append(r)
            if i >= max_item:
                return collected_items
        return collected_items
