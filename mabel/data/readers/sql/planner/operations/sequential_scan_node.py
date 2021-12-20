"""
Sequential Scan Node

This is a SQL Query Execution Plan Node.

This Node reads and parses the data from a blob into a Relation. This Node doesn't try
to use any Index or ZomeMap to improve performance. This Node is intended to be used
in situations where there there are no relevant Index or ZoneMaps available.

As we read, we'll create a Min/Max index - we won't perform full profiling as this is
currently too slow for real-time use. 
"""


            # Read
            record_iterator = self.reader.read_blob(blob_name)
            # Decompress
            record_iterator = decompressor(record_iterator)
            # Parse
            record_iterator = map(parser, record_iterator)