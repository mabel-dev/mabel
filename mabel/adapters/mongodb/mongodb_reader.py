"""
A MongoDB Reader

This is a light-weight and prototype MongoDB reader.
"""
import io
from typing import Iterable, Optional, List
from ...data.readers.internals.base_inner_reader import BaseInnerReader
from ...errors import MissingDependencyError

try:
    import pymongo  # type:ignore

    mongo_installed = True
except ImportError:  # pragma: no cover
    mongo_installed = False


class MongoDbReader(BaseInnerReader):
    def __init__(self, connection_string: str, **kwargs):

        if not mongo_installed:  # pragma: no cover
            raise MissingDependencyError(
                "`pymongo` is missing, please install or include in requirements.txt"
            )

        super().__init__(**kwargs)

        connection = pymongo.MongoClient(connection_string)
        self.database = connection[kwargs.get("dataset")]

        # chunk size affects memory usage
        self.chunk_size: int = kwargs.get("chunk_size", 10)
        self.query: dict = kwargs.get("query", {})

    def get_list_of_blobs(self):
        return self.database.list_collection_names()

    def get_records(
        self, blob_name: str, rows: Optional[Iterable[int]] = None
    ) -> Iterable[str]:
        collection = self.database[blob_name]  # type:ignore
        chunks = self._iterate_by_chunks(
            collection, self.chunk_size, 0, query=self.query
        )
        for docs in chunks:
            yield from docs

    def _iterate_by_chunks(self, collection, chunksize=1, start_from=0, query={}):
        chunks = range(start_from, collection.find(query).count(), int(chunksize))
        num_chunks = len(chunks)
        for i in range(1, num_chunks + 1):
            if i < num_chunks:
                yield collection.find(query)[chunks[i - 1] : chunks[i]]
            else:
                yield collection.find(query)[chunks[i - 1] : chunks.stop]

    def get_blobs_at_path(self, path):
        # not used but must be present
        pass

    def get_blob_stream(self, blob_name: str) -> io.IOBase:
        # not used but must be present
        pass
