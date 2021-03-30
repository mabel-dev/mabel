import os
import sys
import time
import datetime
from pprint import pprint
sys.path.insert(1, os.path.join(sys.path[0], '../..'))
from mabel.data.readers import Reader, MinioReader
from mabel.data.formats import dictset
try:
    from rich import traceback
    traceback.install()
except ImportError:
    pass


try:
    from dotenv import load_dotenv   # type:ignore
    from pathlib import Path
    env_path = Path('.') / '.env'
    load_dotenv(dotenv_path=env_path)
except ImportError:
    pass


reader = Reader(
        thread_count=4,
        reader=MinioReader,
        secure=False,
        end_point=os.getenv('MINIO_END_POINT'),
        access_key=os.getenv('MINIO_ACCESS_KEY'),
        secret_key=os.getenv('MINIO_SECRET_KEY'),
        dataset='SNAPSHOTS/NVD/NVD_CVE_LIST/%datefolders/',
        #row_format='text',
        #start_date=datetime.date(2020, 1, 30),
        #end_date=datetime.date(2020, 2, 5),
        #select=['username'],
        #where=lambda r: b'smb' in r
)

#reader = dictset.limit(reader, 100)

start = time.perf_counter_ns()
count = 0
for count, item in enumerate(reader):
    pass

print(count, (time.perf_counter_ns() - start)/1e9)

#pprint(item)