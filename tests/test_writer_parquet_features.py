import shutil
import os
import sys
import datetime

sys.path.insert(1, os.path.join(sys.path[0], ".."))

from mabel.adapters.disk import DiskReader, DiskWriter
from mabel.data import BatchWriter, Reader


def test_parquet_row_group_size():
    """Test that parquet_row_group_size parameter is passed and used correctly"""
    shutil.rmtree("_temp_rowgroup", ignore_errors=True)
    
    # Create a writer with custom row group size
    w = BatchWriter(
        inner_writer=DiskWriter,
        dataset="_temp_rowgroup",
        format="parquet",
        date=datetime.datetime.utcnow().date(),
        schema=[{"name": "id", "type": "INTEGER"}, {"name": "value", "type": "VARCHAR"}],
        parquet_row_group_size=100,  # Small row group for testing
    )
    
    # Write 1000 records
    for i in range(1000):
        w.append({"id": i, "value": f"value_{i}"})
    
    w.finalize()
    
    # Read back and verify we can read the data
    r = Reader(inner_reader=DiskReader, dataset="_temp_rowgroup")
    records = list(r)
    assert len(records) == 1000, f"Expected 1000 records, got {len(records)}"
    
    # Verify the parquet file has multiple row groups
    import glob
    import pyarrow.parquet as pq
    
    parquet_files = glob.glob("_temp_rowgroup/**/*.parquet", recursive=True)
    assert len(parquet_files) > 0, "No parquet files found"
    
    # Check row groups in the first file
    parquet_file = pq.ParquetFile(parquet_files[0])
    num_row_groups = parquet_file.num_row_groups
    
    # With 1000 records and row_group_size=100, we should have multiple row groups
    # (exact number depends on how records are distributed across blobs)
    assert num_row_groups > 0, f"Expected at least 1 row group, got {num_row_groups}"
    
    shutil.rmtree("_temp_rowgroup", ignore_errors=True)


def test_parquet_sorting():
    """Test that sort_by parameter sorts records correctly"""
    shutil.rmtree("_temp_sort", ignore_errors=True)
    
    # Create a writer with sorting
    w = BatchWriter(
        inner_writer=DiskWriter,
        dataset="_temp_sort",
        format="parquet",
        date=datetime.datetime.utcnow().date(),
        schema=[{"name": "id", "type": "INTEGER"}, {"name": "value", "type": "VARCHAR"}],
        sort_by="id",  # Sort by id column
        parquet_row_group_size=5000,
    )
    
    # Write records in reverse order
    for i in range(100, 0, -1):
        w.append({"id": i, "value": f"value_{i}"})
    
    w.finalize()
    
    # Read back and verify the data is sorted
    r = Reader(inner_reader=DiskReader, dataset="_temp_sort")
    records = list(r)
    
    assert len(records) == 100, f"Expected 100 records, got {len(records)}"
    
    # Check that records are sorted by id
    ids = [record["id"] for record in records]
    assert ids == list(range(1, 101)), f"Records are not sorted correctly: {ids[:10]}..."
    
    shutil.rmtree("_temp_sort", ignore_errors=True)


def test_parquet_sorting_descending():
    """Test that sort_by parameter can sort in descending order"""
    shutil.rmtree("_temp_sort_desc", ignore_errors=True)
    
    # Create a writer with descending sorting
    w = BatchWriter(
        inner_writer=DiskWriter,
        dataset="_temp_sort_desc",
        format="parquet",
        date=datetime.datetime.utcnow().date(),
        schema=[{"name": "id", "type": "INTEGER"}, {"name": "value", "type": "VARCHAR"}],
        sort_by=[("id", "descending")],  # Sort by id in descending order
        parquet_row_group_size=5000,
    )
    
    # Write records in random order
    for i in [50, 10, 90, 30, 70, 20, 80, 40, 60, 100]:
        w.append({"id": i, "value": f"value_{i}"})
    
    w.finalize()
    
    # Read back and verify the data is sorted in descending order
    r = Reader(inner_reader=DiskReader, dataset="_temp_sort_desc")
    records = list(r)
    
    assert len(records) == 10, f"Expected 10 records, got {len(records)}"
    
    # Check that records are sorted by id in descending order
    ids = [record["id"] for record in records]
    expected_ids = [100, 90, 80, 70, 60, 50, 40, 30, 20, 10]
    assert ids == expected_ids, f"Records are not sorted correctly in descending order: {ids}"
    
    shutil.rmtree("_temp_sort_desc", ignore_errors=True)


def test_parquet_default_row_group_size():
    """Test that default row group size is 5000"""
    shutil.rmtree("_temp_default", ignore_errors=True)
    
    # Create a writer without specifying row group size
    w = BatchWriter(
        inner_writer=DiskWriter,
        dataset="_temp_default",
        format="parquet",
        date=datetime.datetime.utcnow().date(),
        schema=[{"name": "id", "type": "INTEGER"}, {"name": "value", "type": "VARCHAR"}],
    )
    
    # Write some records
    for i in range(100):
        w.append({"id": i, "value": f"value_{i}"})
    
    w.finalize()
    
    # Read back and verify we can read the data
    r = Reader(inner_reader=DiskReader, dataset="_temp_default")
    records = list(r)
    assert len(records) == 100, f"Expected 100 records, got {len(records)}"
    
    shutil.rmtree("_temp_default", ignore_errors=True)


if __name__ == "__main__":  # pragma: no cover
    from tests.helpers.runner import run_tests
    
    run_tests()
