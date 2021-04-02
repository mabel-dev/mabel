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


value = '~~kn~~'

t = time.time_ns()
for rep in range(500):
    r = binary(value)
print('binary', (time.time_ns() - t) / 1e9, r)
