"""
Speed test to see if moving to a specialized data format would provide
performance benefits - we're focusing just on read speed.

for 100,000 records:
avro took 4.2137237 seconds
jsonl took 0.525919 seconds

outcome: stay with the simple JSONL format.
"""

import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))
from mabel.data.readers import Reader, FileReader
from mabel.data.formats import dictset

try:
    from rich import traceback

    traceback.install()
except ImportError:  # pragma: no cover
    pass

# schema = avro.schema.parse(open("twitter.avsc").read())
# writer = DataFileWriter(open("twitter.avro", "wb"), DatumWriter(), schema)

# for record in dictset.limit(reader, 100000):
#    writer.append(record)

# writer.close()

# import json
# with open('twitter2.jsonl', 'w', encoding='utf8') as j:
#    reader = DataFileReader(open("twitter.avro", "rb"), DatumReader())
#    for tweet in reader:
#        j.write(json.dumps(tweet) + '\n')
#    print(user)
#    print('===================')
#    reader.close()

from timer import Timer

reader = DataFileReader(open("twitter.avro", "rb"), DatumReader())
with Timer("avro"):
    print(len(list(dictset.select_from(reader, where=lambda t: "trump" in t["text"]))))
reader.close

reader = FileReader(dataset="./twitter.jsonl").read_from_source("twitter.jsonl")
with Timer("jsonl"):
    reader = dictset.jsonify(reader)
    print(len(list(dictset.select_from(reader, where=lambda t: "trump" in t["text"]))))
reader.close
