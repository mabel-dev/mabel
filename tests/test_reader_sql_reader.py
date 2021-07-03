import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.data import Reader
from mabel.data import BatchWriter
from mabel.adapters.disk import DiskWriter, DiskReader
from mabel.data.readers.internals.alpha_sql_reader import SqlReader
import shutil
from rich import traceback

traceback.install()


def do_writer():
    w = BatchWriter(inner_writer=DiskWriter, dataset="_temp/twitter", raw_path=True)
    r = Reader(inner_reader=DiskReader, dataset="tests/data/tweets", raw_path=True)
    for tweet in r:
        w.append(tweet)
    w.finalize()


def test_most_basic_sql():

    shutil.rmtree("_temp", ignore_errors=True)
    do_writer()
    s = SqlReader("SELECT * FROM _temp.twitter", inner_reader=DiskReader, raw_path=True)
    findings = list(s)
    assert len(findings) == 50, len(findings)
    shutil.rmtree("_temp", ignore_errors=True)


# fmt:off
SQL_TESTS = [
    {"statement":"SELECT * FROM tests.data.index.is  WHERE user_name = 'Verizon Support'", "result":2},
    {"statement":"SELECT * FROM tests.data.index.not WHERE user_name = 'Verizon Support'", "result":2},
    {"statement":"SELECT * FROM tests.data.index.is  WHERE user_name = '*******'", "result":0},
    {"statement":"SELECT * FROM tests.data.index.is  WHERE user_name LIKE '_erizon _upport'", "result":2},
    {"statement":"SELECT * FROM tests.data.index.is  WHERE user_name LIKE '%Support%'", "result":31},
    {"statement":"SELECT * FROM tests.data.index.is  WHERE user_name = 'Verizon Support'", "result":2},
    {"statement":"SELECT * FROM tests.data.index.is  WHERE tweet_id = 1346604539923853313", "result":1}, 
    {"statement":"SELECT * FROM tests.data.index.is  WHERE tweet_id = 1346604539923853313 and user_id = 4832862820", "result":1},
    {"statement":"SELECT * FROM tests.data.index.is  WHERE tweet_id = 1346604539923853313 or user_id = 2147860407", "result":2},
    {"statement":"SELECT * FROM tests.data.index.is  WHERE tweet_id = 1346604539923853313 or user_verified = True", "result":453},
    {"statement":"SELECT * FROM tests.data.index.is  WHERE user_name = 'Dave Jamieson' and user_verified = True", "result":1},
]
# fmt:on


def test_sql():

    for test in SQL_TESTS:
        s = SqlReader(
            test.get("statement"),
            inner_reader=DiskReader,
            raw_path=True,
        )
        findings = list(s)
        assert len(findings) == test.get(
            "result"
        ), f"{test.get('statement')} == {len(findings)}"


if __name__ == "__main__":  # pragma: no cover
    test_sql()
    test_most_basic_sql()

    print("okay")
