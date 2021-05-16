"""
Bit Array

(C) 2021 Justin Joyce.

This is a limited drop-in replacement for the bitarry library which may not
work on all environments due to needing a C compiler to be present.

If bitarry is not installed, this will take it's place, but it is only
a partial implementation and is many times slower.


Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""


class bitarray:
    def __init__(self, size: int = 0, endian: str = ""):
        self.size = size
        self.bits = [0 for i in range((size + 7) >> 3)]

    def setall(self, value=0):
        for i in range(self.size):
            self.__setitem__(i, value)

    def __setitem__(self, bit, value):
        b = self.bits[bit >> 3]
        if value:
            self.bits[bit >> 3] = b | 1 << (bit % 8)
        else:
            self.bits[bit >> 3] = b & ~(1 << (bit % 8))

    def __getitem__(self, bit):
        b = self.bits[bit >> 3]
        return (b >> (bit % 8)) & 1

    def __repr__(self):
        def _inner():
            for i in range(self.size):
                yield str(self[i])

        return f"bitarray('{''.join(list(_inner()))}')"

    def tofile(self, filehandle):
        filehandle.write(bytes(self.bits))

    def fromfile(self, filehandle):
        self.size = 0
        b = filehandle.read(1)
        while b:
            i = int.from_bytes(b, byteorder="big")
            self.size += 8
            self.bits.append(i)
            b = filehandle.read(1)
