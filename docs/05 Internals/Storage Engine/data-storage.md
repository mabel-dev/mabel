
The 'raw' data is JSONL in 64Mb chunks with ZSTD compression. The database-engine MUST
be able to support this format (and other 'raw' formats) to be broadly useful.

A side-car file for each dataset (the folder of partitions) provides information to
optimize database performance, a .map file. This is a JSON file which contains schema
and zonemap information. 

The schema is used to speed up reading into a Relation