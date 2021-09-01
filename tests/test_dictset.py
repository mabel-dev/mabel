import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel import Reader, DictSet
from mabel.data import STORAGE_CLASS
from mabel.adapters.disk import DiskReader


STORAGE_CLASSES = [
    STORAGE_CLASS.NO_PERSISTANCE,
    STORAGE_CLASS.COMPRESSED_MEMORY,
    STORAGE_CLASS.BINARY_DISK,
    STORAGE_CLASS.MEMORY,
    STORAGE_CLASS.DISK]

def get_ds(**kwargs):
    ds = Reader(inner_reader=DiskReader, dataset="tests/data/tweets", raw_path=True, **kwargs)
    return ds

def test_count():
    for storage_class in STORAGE_CLASSES:
        ds = get_ds(persistence=storage_class)
        if storage_class == STORAGE_CLASS.NO_PERSISTANCE:
            assert ds.count() == -1, f"{storage_class} {ds.count()}"
        else:
            assert ds.count() == 50,  f"{storage_class} {ds.count()}"

def test_enumeration():
    for storage_class in STORAGE_CLASSES:
        ds = get_ds(persistence=storage_class)
        i = -1
        for i, r in enumerate(ds):
            pass
        assert i+1 == 50,  f"{storage_class} {i+1}"

def test_sample():
    for storage_class in STORAGE_CLASSES:
        ds = get_ds(persistence=storage_class)
        sample = ds.sample(0.02)
        assert isinstance(sample, DictSet)
        assert sample.count() < 5, sample.count()
        assert sample.storage_class == storage_class

def test_repr():
    for storage_class in STORAGE_CLASSES:
        ds = get_ds(persistence=storage_class)
        rep = repr(ds)
        assert "├" in rep

if __name__ == "__main__":
    test_count()
    test_enumeration()
    test_sample()
    test_repr()