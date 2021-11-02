import cProfile
import pyximport

pyximport.install()


def do_read():

    import os
    import sys

    sys.path.insert(1, os.path.join(sys.path[0], "../.."))
    from mabel.data.internals.group_by import GroupBy
    from mabel.data.internals.dictset import STORAGE_CLASS, DictSet

    data = [
        {"user": "bob", "value": 1},
        {"user": "bob", "value": 2},
        {"user": "bob", "value": 1},
        {"user": "bob", "value": 2},
        {"user": "bob", "value": 1},
        {"user": "alice", "value": 3},
        {"user": "alice", "value": 4},
        {"user": "alice", "value": 3},
        {"user": "alice", "value": 4},
        {"user": "alice", "value": 5},
        {"user": "alice", "value": 5},
        {"user": "eve", "value": 6},
        {"user": "eve", "value": 7},
    ] * 10000

    for i in range(50):
        ds = DictSet(data, storage_class=STORAGE_CLASS.MEMORY)
        gs = list(GroupBy(ds, "user").min("value"))


cProfile.run("do_read()", "profile.txt")

import pstats

p = pstats.Stats("profile.txt")
p.sort_stats("tottime").print_stats(50)

# do_read()
