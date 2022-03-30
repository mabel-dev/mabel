"""
The WriterPool maintains multiple BlobWriters.

It allows new ParitionWriters be be added to the pool, and when the size of the
pool exceeds the stated size limit, it can recommend the least recently used
BlobWriters to be evicted. Note the pool_size is not a hard limit, there
can be more active BlobWriters, this is used to determine how many
BlobWriters to recommend for evict.
"""
import time
import threading
from .blob_writer import BlobWriter


class WriterPool:

    __slots__ = ("writers", "pool_size", "kwargs")

    def __init__(self, pool_size: int = 5, **kwargs):

        self.writers: list = []
        self.pool_size = pool_size
        self.kwargs = kwargs.copy()
        # remove this from the kw args if it's there
        self.kwargs.pop("dataset", "")

    def get_writer(self, identity):
        try:
            # if a writer with the specified identity exists then return it
            # update the last_access status so we can determin the LRU
            writer = [w for w in self.writers if w["identity"] == identity].pop()
            writer["last_access"] = time.time()
            return writer["writer"]
        except IndexError:
            # if no writer was found, create a new one and add to the pool.
            # we don't check if we're overflowing before doing this,
            # pool_size is managed by the background thread in the Writer
            writer = self.create_writer(identity)
            self.writers.append(writer)
            return writer.get("writer")

    def create_writer(self, identity):
        # instantiate a new writer
        new_writer = {
            "identity": identity,
            "last_access": time.time(),
            "writer": BlobWriter(dataset=identity, **self.kwargs),
        }
        return new_writer

    def remove_writer(self, identity):
        # remove a writer from the pool and commit
        lock = threading.Lock()
        try:
            lock.acquire(blocking=True, timeout=10)
            writers = [w for w in self.writers if w.get("identity") == identity]
            if len(writers) != 1:
                import mabel.logging

                logger = mabel.logging.get_logger()
                logger.error(
                    f"Unable to find writer to remove - indentity={identity}, poolsize={len(self.writers)}"
                )
            else:
                writer = writers[0]
                self.writers = [
                    w for w in self.writers if w.get("identity") != identity
                ]
                writer.get("writer").commit()
        finally:
            lock.release()

    def close(self):
        # evict everyone from the pool
        lock = threading.Lock()
        try:
            lock.acquire(blocking=True, timeout=10)
            [self.remove_writer(writer.get("identity")) for writer in self.writers]
            self.writers = []
        finally:
            lock.release()

    def nominate_writers_to_evict(self):
        # if there are more than pool_size writers in the pool, get a list of
        # LRU ones that exceed the pool_size
        def _inner_sort_key(row):
            return row.get("last_access")

        evictees = sorted(self.writers, key=_inner_sort_key, reverse=False)[
            : len(self.writers) - self.pool_size
        ]
        return [writer.get("identity") for writer in evictees]

    def get_stale_writers(self, seconds_since_last_use):
        """get the identities of writers which haven't been accessed recently"""
        now = time.time()
        stale_writers = [
            writer.get("identity")
            for writer in self.writers
            if now - writer.get("last_access") > seconds_since_last_use
        ]
        return stale_writers

    def __str__(self):
        # much easier to read
        return display.ascii_table(self.writers)
