import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
import shutil
from mabel.data import STORAGE_CLASS
from mabel.data import Reader
from mabel.data import BatchWriter
from mabel.adapters.disk import DiskWriter, DiskReader
from mabel.data.readers.internals.sql_reader import SqlReader
from rich import traceback

traceback.install()


def test_where():

    s = SqlReader(
        "SELECT * FROM tests.data.tweets WHERE username == 'BBCNews'",
        inner_reader=DiskReader,
        raw_path=True,
        persistence=STORAGE_CLASS.MEMORY,
    )
    assert s.count() == 6, s.count()


# fmt:off
SQL_TESTS = [
    {"statement":"SELECT * FROM tests.data.index.is", "result":65499},
    {"statement":"SELECT * FROM tests.data.index.is  WHERE user_name = 'Verizon Support'", "result":2},
    {"statement":"select * from tests.data.index.is  where user_name = 'Verizon Support'", "result":2},
    {"statement":"SELECT * FROM tests.data.index.not WHERE user_name = 'Verizon Support'", "result":2},
    {"statement":"SELECT * FROM tests.data.index.is  WHERE user_name = '********'", "result":0},
    {"statement":"SELECT * FROM tests.data.index.is  WHERE user_name LIKE '_erizon _upport'", "result":2},
    {"statement":"SELECT * FROM tests.data.index.is  WHERE user_name LIKE '%Support%'", "result":31},
    {"statement":"SELECT * FROM tests.data.index.is  WHERE user_name = 'Verizon Support'", "result":2},
    {"statement":"SELECT * FROM tests.data.index.is  WHERE tweet_id = 1346604539923853313", "result":1}, 
    {"statement":"SELECT * FROM tests.data.index.is  WHERE tweet_id = 1346604539923853313 AND user_id = 4832862820", "result":1},
    {"statement":"SELECT * FROM tests.data.index.is  WHERE tweet_id IN (1346604539923853313, 1346604544134885378)", "result":2},
    {"statement":"SELECT * FROM tests.data.index.is  WHERE tweet_id = 1346604539923853313 OR user_id = 2147860407", "result":2},
    {"statement":"SELECT * FROM tests.data.index.is  WHERE tweet_id = 1346604539923853313 OR user_verified = True", "result":453},
    {"statement":"SELECT * FROM tests.data.index.is  WHERE user_name = 'Dave Jamieson' AND user_verified = True", "result":1},
    {"statement":"SELECT COUNT(*) FROM tests.data.index.is  WHERE user_name = 'Dave Jamieson' AND user_verified = True", "result":1},
    {"statement":"SELECT COUNT(*) FROM tests.data.index.is GROUP BY user_verified", "result":-1},  # there's two groups in a generator
    {"statement":"SELECT * FROM tests.data.index.is WHERE hash_tags contains 'Georgia'", "result":50},
]
# fmt:on


def test_sql():

    for test in SQL_TESTS:
        s = SqlReader(
            test.get("statement"),
            inner_reader=DiskReader,
            raw_path=True,
            persistence=STORAGE_CLASS.MEMORY,
        )
        # print(s)
        assert s.count() == test.get(
            "result"
        ), f"{test.get('statement')} == {s.count()}"


def test_sql_to_dictset():

    s = SqlReader(
        sql_statement="SELECT * FROM tests.data.index.not",
        inner_reader=DiskReader,
        raw_path=True,
        persistence=STORAGE_CLASS.MEMORY,
    )
    keys = s.keys()
    assert "tweet_id" in keys, keys
    assert "text" in keys, keys
    assert "followers" in keys, keys
    assert len(s.take(10).collect()) == 10


def test_select():

    s = SqlReader(
        sql_statement="SELECT tweet_id, user_name FROM tests.data.index.not",
        inner_reader=DiskReader,
        raw_path=True,
    )
    first = s.first()
    assert first.get("tweet_id") is not None, first
    assert first.get("user_name") is not None, first
    assert first.get("timestamp") is None, first
    


def test_limit():

    s = SqlReader(
        sql_statement="SELECT tweet_id, user_name FROM tests.data.index.not LIMIT 12",
        inner_reader=DiskReader,
        raw_path=True,
        persistence=STORAGE_CLASS.MEMORY,
    )
    record_count = s.count()
    assert record_count == 12, record_count


def test_group_by_count():

    s = SqlReader(
        sql_statement="SELECT COUNT(*) FROM tests.data.index.not GROUP BY user_verified",
        inner_reader=DiskReader,
        raw_path=True,
    )
    records = s.collect()
    record_count = len(records)
    assert record_count == 2, record_count

    s = SqlReader(
        sql_statement="SELECT COUNT(*) FROM tests.data.index.not GROUP BY user_name",
        inner_reader=DiskReader,
        raw_path=True,
    )
    records = s.collect()
    record_count = len(records)
    assert record_count == 56527, record_count


if __name__ == "__main__":  # pragma: no cover
    test_sql()
    test_sql_to_dictset()
    test_select()
    test_where()
    test_limit()
    test_group_by_count()

    print("okay")
