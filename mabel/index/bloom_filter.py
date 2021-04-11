"""
Bloom Filter

This is a variation of the Bloom Filter; this provides a fast and memory
efficient way to tell if an item is in a list.

https://en.wikipedia.org/wiki/Bloom_filter

(C) 2021 Justin Joyce.

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

from bitarray import bitarray  # type:ignore
import mmh3  # type:ignore


class BloomFilter():

    __slots__ = ('filter_size', 'hash_count', 'bits')

    def __init__(
            self,
            number_of_elements: int = 50000,
            fp_rate: float = 0.05):
        self.filter_size = BloomFilter.get_size(number_of_elements, fp_rate)
        self.hash_count = BloomFilter.get_hash_count(self.filter_size, number_of_elements)
        self.bits = bitarray(self.filter_size, endian='big')
        self.bits.setall(0)

    @staticmethod
    def _log(x):
        return 99999999*(x**(1/99999999)-1)

    @staticmethod
    def get_size(number_of_elements, fp_rate):
        """
        Calculate the size of the bitarray

        Parameters:
            number_of_elements: integer
                The number of items expected to be stored in filter
            fp_rate: float (optional)
                False Positive rate (0 to 1), default 0.95

        Returns:
            integer
        """
        m = -(number_of_elements * BloomFilter._log(fp_rate))/(BloomFilter._log(2)**2) + 1
        return int(m)

    @staticmethod
    def get_hash_count(filter_size, number_of_elements):
        """
        Calculate the number of hashes to use to identify elements
 
        Parameters:
            filter_size: integer
                The size of the filter bit array
            number_of_elements: integer
                The number of items expected to be stored in filter

        Returns:
            integer
        """
        k = (filter_size/number_of_elements) * BloomFilter._log(2)
        return max(int(k), 2)

    def add(self, term):
        for i in range(self.hash_count):
            h = mmh3.hash(term, seed=i) % self.filter_size
            self.bits[h] = 1

    def __contains__(self, term):
        for i in range(self.hash_count):
            h = mmh3.hash(term, seed=i) % self.filter_size
            if self.bits[h] == 0:
                return False
        return True

def write_bloom_filter(bf: BloomFilter, filename: str):
    with open(filename, 'wb') as fh:
        fh.write(bf.filter_size.to_bytes(4, byteorder='big'))
        fh.write(bf.hash_count.to_bytes(4, byteorder='big'))
        bf.bits.tofile(fh)

def read_bloom_filter(
        filename: str,
        number_of_elements: int = 50000):
    
    def bytes_to_int(xbytes: bytes) -> int:
        return int.from_bytes(xbytes, 'big')

    bits = bitarray(endian='big')
    with open(filename, 'rb') as fh:
        filter_size = bytes_to_int(fh.read(4))
        hash_count  = bytes_to_int(fh.read(4))
        bits.fromfile(fh)

    bf = BloomFilter()
    bf.bits = bits
    bf.filter_size = filter_size
    bf.hash_count = hash_count
    return bf
