"""
There's lots of things to time the duration of, this helps.

~~~
with Timer():
    the thing to time
~~~
"""

import time


class Timer(object):
    def __init__(self, name="test"):
        self.name = name

    def __enter__(self):
        self.start = time.perf_counter()

    def __exit__(self, type, value, traceback):
        print("{} took {} seconds".format(self.name, time.perf_counter() - self.start))
