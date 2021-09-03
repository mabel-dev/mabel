"""
Separate the implementation of the Cursor from the Reader.

Cursor is made of three parts:
- map      : a bit array representing all of the blobs in the set - unread blobs
             are 0s and read blobs are 1s. This allows for blobs to be read in 
             an arbitrary order - although currently only implemented linearly.
- partition: the active parition (blob) that is being read
- location : the record in the active partition (blob), so we can resume reading
             midway through the blob if required.
"""
from multiprocessing import Value
from bitarray import bitarray
from siphashc import siphash
import orjson

class InvalidCursor(Exception):
    pass

class Cursor():

    def __init__(self, readable_blobs):
        # sort the readable blobs so they are in a consistent order
        self.readable_blobs = sorted(readable_blobs)
        self.read_blobs = []
        self.partition = ''
        self.location = -1

    def load_cursor(self, cursor):
        if isinstance(cursor, str):
            cursor = orjson.loads(cursor)

        if not "location" in cursor.keys() or not "map" in cursor.keys() or not "partition" in cursor.keys():
            raise InvalidCursor(f"Cursor is malformed or corrupted {cursor}")

        self.location = cursor["location"]
        find_partition = [blob for blob in self.readable_blobs if siphash('%'*16, blob) == cursor['partition']]
        if len(find_partition) == 1:
            self.partition = find_partition[0]
        map_bytes = bytes.fromhex(cursor["map"])
        blob_map = bitarray()
        blob_map.frombytes(map_bytes)
        self.read_blobs = [self.readable_blobs[i] for i in range(len(self.readable_blobs)) if blob_map[i]]

    def mark_as_read(self, blob_name):
        self.read_blobs.append(blob_name)

    def get_next_blob(self):
        unread = [blob for blob in self.readable_blobs if blob not in self.read_blobs]
        if len(unread) > 0:
            self.partition = unread[0]
            self.location = -1
            return self.partition
        return None

    def get(self):
        blob_map = bitarray(''.join(['1' if blob in self.read_blobs else '0' for blob in self.readable_blobs]))
        return {
            "map": blob_map.tobytes().hex(),
            "partition": siphash('%'*16, self.partition),
            "location": self.location
        }

if __name__ == "__main__":

    rb = ["a", "b", "c", "d", "e", "f", "g"]
    c = Cursor(rb)
    print(c.get())

    b = c.get_next_blob()
    while b:
        print(b)
        c.mark_as_read(b)
        print(c.get())
        nc = Cursor(rb)
        nc.load_cursor(c.get())
        print(nc.read_blobs)
        b = c.get_next_blob()

