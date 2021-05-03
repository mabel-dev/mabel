"""
Test the MQTT reader, not available in most environments
"""
import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from mabel.adapters.mongodb import MongoDbReader
from mabel.data import Reader
from mabel.data.formats import dictset
from rich import traceback

traceback.install()


def test_mongo():

    reader = Reader(
        connection_string=os.getenv('MONGO_CONNECTION_STRING'),
        dataset='twitter',
        inner_reader=MongoDbReader,
        row_format='pass-thru'
    )

    reader = dictset.limit(reader, 10)
    for i, item in enumerate(reader):
        print(i, type(item), item)


if __name__ == "__main__":  # pragma: no cover
    test_mongo()

    print('okay')
