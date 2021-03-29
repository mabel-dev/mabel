from pyarrow import json
import pyarrow.parquet as pq
import pyarrow as pa
from glob import glob

import time
t = time.time_ns()

fn = 'tests/data/formats/tweets'
table = json.read_json(fn + '.jsonl')
pq.write_table(table, 'parc.parquet', compression='ZSTD')
#table = pq.read_table(fn + '.parquet')

#print(table.schema)
#print(table.column_names)
#print(table.select(['username']))
#print(table.take([0]).to_pydict())
print(table.take([0,500,9000])['username'])
#print(table.filter())



def iter_arrow(tbl):
    for batch in tbl.to_batches():
        dict_batch = batch.to_pydict()
        for index in range(len(batch)):
            yield {k:v[index] for k,v in dict_batch.items()}



#, filter=([('user_verified', '=', True)]))
#table = pq.read_table(fn + '.parquet')

#for i, r in enumerate(iter_arrow(table)):
#    pass

print ((time.time_ns() - t) / 1e9)


