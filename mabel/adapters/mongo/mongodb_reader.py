"""
A MongoDB Reader

This is a light-weight MongoDB reader to fulfil a specific purpose,
it needs some work to make it fully reusable.
"""
from typing import Iterator, Tuple, Optional, List
import datetime
from ...data.readers.internals.base_inner_reader import BaseInnerReader
try:
    import pymongo     # type:ignore
except ImportError:    # pragma: no cover
    pass


class MongoDbReader(BaseInnerReader):

    def __init__(
            self,
            connection_string: str,
            database: str,
            **kwargs):

        raise NotImplementedError('MongoDbReader needs to be updated')
        
        #connection = pymongo.MongoClient(connection_string)
        #self.database = connection[database] 

        # chunk size affects memory usage
        #self.chunk_size: int = kwargs.get('chunk_size', 10000)
        #self.query: dict = kwargs.get('query', {})


    def list_of_sources(self):
        yield from self.database.list_collection_names()


    def read_from_source(self, item: str):
        collection = self.database[item]  # type:ignore
        chunks = self._iterate_by_chunks(
                collection,
                self.chunk_size,
                0,
                query=self.query)
        for docs in chunks:
            yield from docs


    def _iterate_by_chunks(self, collection, chunksize=1, start_from=0, query={}):
        chunks = range(start_from, collection.find(query).count(), int(chunksize))
        num_chunks = len(chunks)
        for i in range(1,num_chunks+1):
            if i < num_chunks:
                yield collection.find(query)[chunks[i-1]:chunks[i]]
            else:
                yield collection.find(query)[chunks[i-1]:chunks.stop]
