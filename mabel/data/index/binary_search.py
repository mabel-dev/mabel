"""



Adapted from http://pts.github.io/pts-line-bisect/line_bisect_evolution.html

License: GNU GPL v2 or newer, at your choice.
Accessed: 2021-03-06
"""


import orjson as json
from functools import lru_cache


@lru_cache(8)
def _read_and_compare(ofs, f, size, tester):
    if ofs:
        if f.tell() != ofs - 1:  # Avoid lseek(2) call if not needed.
            f.seek(ofs - 1)  # Just to figure out where our line starts.
        f.readline()  # Ignore previous line, find our line.
        fofs = min(size, f.tell())
    else:
        fofs = 0
    g = True
    if fofs < size:
        if not fofs and f.tell():
            f.seek(0)
        line = f.readline()
        line = json.loads(line)
        line = line['key']
        g = tester <= line
    return [fofs, g, ofs]

def bisect_way(f, x):
    f.seek(0, 2)
    size = f.tell()
    lo, hi, mid = 0, size - 1, 1
    while lo < hi:
        mid = (lo + hi) >> 1
        midf, g, _ = _read_and_compare(mid, f, size, x)
        if g:
            hi = mid
        else:
            lo = mid + 1
        if mid != lo:
            midf = _read_and_compare(lo, f, size, x)[0]
    return midf




import sys
import time

def binary(value):
    with open('sorted_twitter.index', 'r') as f:
        start = bisect_way(f, value)
        f.seek(start)
        data = f.readline()
        return data
        #sys.stdout.write(data + '\n')

def scan(value):
    with open('sorted_twitter.index', 'r') as f:
        for r in f:
            j = json.loads(r)
            if j["key"] == value:
                return r
                #sys.stdout.write(r)

def arrow(value):
    import pyarrow.json as js  # type:ignore
    table = js.read_json('sorted_twitter.index')
    
    mid = 0
    start = 0
    end = len(table)

    while (start <= end):
        mid = (start + end) >> 1
        mid_value = str(table.take([mid])[0][0])
        if value == mid_value:
            return  {k:v[0] for k,v in table.take([mid]).to_pydict().items()}
        if value < mid_value:
            end = mid - 1
        else:
            start = mid + 1
    return -1

def arrow_2(value):
    import pyarrow.json as js  # type:ignore
    table = js.read_json('sorted_twitter.index')

    column = table.column('key')
    
    mid = 0
    start = 0
    end = len(table)

    while (start <= end):
        mid = (start + end) >> 1
        mid_value = str(column[mid])
        if value == mid_value:
            return  {k:v[0] for k,v in table.take([mid]).to_pydict().items()}
        if value < mid_value:
            end = mid - 1
        else:
            start = mid + 1
    return -1

from btree import BTree  # type:ignore
def btree(value):
    idx = BTree.read_file('sorted_twitter.index')
    return idx.retrieve(value)

value = '~~kn~~'

t = time.time_ns()
for rep in range(500):
    r = binary(value)
print('binary', (time.time_ns() - t) / 1e9, r)

quit()

t = time.time_ns()
for rep in range(2):
    r = scan(value)
print('scan', (time.time_ns() - t) / 1e9, r)

t = time.time_ns()
for rep in range(2):
    r = arrow(value)
print('arrow', (time.time_ns() - t) / 1e9, r)

t = time.time_ns()
for rep in range(10):
    r = arrow_2(value)
print('arrow_2', (time.time_ns() - t) / 1e9, r)

t = time.time_ns()
for rep in range(1):
    r = btree(value)
print('btree', (time.time_ns() - t) / 1e9, r)