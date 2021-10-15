import cProfile


def do_read():
    import os
    import sys

    sys.path.insert(1, os.path.join(sys.path[0], "../.."))
    from mabel.adapters.disk import DiskReader
    from mabel.data import Reader, SqlReader

    # d = Reader(inner_reader=DiskReader, dataset="tests/data/nvd/", raw_path=True)
    SQL = "SELECT COUNT(*) FROM (SELECT * FROM tests/data/nvd/ GROUP BY cve.CVE_data_meta.ASSIGNER)"
    #SQL = "SELECT COUNT(*) FROM tests.data.index.is GROUP BY user_verified"
    #SQL = "SELECT * FROM tests.data.index.is  WHERE `user_name` = 'Verizon Support'"
    d = SqlReader(SQL, inner_reader=DiskReader, raw_path=True)

    i = -1
    for i, r in enumerate(d):
        print(r)
    print(i + 1)


cProfile.run("do_read()", "profile.txt")

import pstats

p = pstats.Stats("profile.txt")
p.sort_stats("cumulative").print_stats(10)
