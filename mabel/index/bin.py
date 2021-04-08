import struct

RECORD_SIZE = 40

def binary_search(f, target):
    left, right = 0, 99999 #len(nums)-1       # define search space: left, right, and middle

    while left <= right:
        middle = (left + right) >> 1

        f.seek(RECORD_SIZE * middle)
        key_len, key, index = struct.unpack("i 32s i", f.read(RECORD_SIZE))
        key = key.decode()[:key_len]

        if key == target:
            return index
        elif key > target:
            right = middle - 1
        else:
            left = middle + 1
    return -1

def write_index():
    import ujson as json
    by = []
    with open('tests/data/formats/tweets.bin', 'wb') as bi:
        with open('tests/data/formats/tweets.index', 'r') as f:
            for i, row in enumerate(f):
                j = json.loads(row)
                by = struct.pack("i 32s i", len(j['key']), j['key'].encode()[:32], i)
                bi.write(by)
    print(len(by))

#write_index()

import time
value = '~~k~~'

def binbin(value):
    with open('tests/data/formats/tweets.bin', 'rb') as f:
        return binary_search(f, value)

t = time.time_ns()
for rep in range(10000):   # ~20x faster
    r = binbin(value)
print('full binary', (time.time_ns() - t) / 1e9, r)

t = time.time_ns()
with open('tests/data/formats/tweets.bin', 'rb') as f:
    for rep in range(20000):   # ~40x faster
        binary_search(f, value)
print('full binary', (time.time_ns() - t) / 1e9, r)