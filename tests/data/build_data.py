import datetime
import sys
import os

sys.path.insert(1, os.path.join(sys.path[0], "../.."))
from mabel.data import BatchWriter
from mabel.adapters.local import FileWriter, FileReader

try:
    from rich import traceback

    traceback.install()
except ImportError:  # pragma: no cover
    pass


def do_writer():
    w = BatchWriter(
        inner_writer=FileWriter, dataset="tests/data/framed", date=datetime.datetime.utcnow().date()
    )
    for i in range(int(1e5)):
        w.append({"test": 2})
    w.finalize()


do_writer()
fr = FileReader(dataset="tests/data/framed")

print(list(fr.get_list_of_blobs()))
