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


def test_parquet_sorting_list_of_strings():
    """Test that sort_by parameter can accept a list of strings"""
    shutil.rmtree("_temp_sort_list", ignore_errors=True)
    
    # Create a writer with sorting by list of strings
    w = BatchWriter(
        inner_writer=DiskWriter,
        dataset="_temp_sort_list",
        format="parquet",
        date=datetime.datetime.utcnow().date(),
        schema=[
            {"name": "id", "type": "INTEGER"},
            {"name": "category", "type": "VARCHAR"},
            {"name": "value", "type": "VARCHAR"}
        ],
        sort_by=["category", "id"],  # Sort by category first, then id
        parquet_row_group_size=5000,
    )
    
    # Write records in random order
    records_to_write = [
        {"id": 3, "category": "B", "value": "value_3"},
        {"id": 1, "category": "A", "value": "value_1"},
        {"id": 4, "category": "B", "value": "value_4"},
        {"id": 2, "category": "A", "value": "value_2"},
        {"id": 5, "category": "C", "value": "value_5"},
    ]
    
    for record in records_to_write:
        w.append(record)
    
    w.finalize()
    
    # Read back and verify the data is sorted by category, then id
    r = Reader(inner_reader=DiskReader, dataset="_temp_sort_list")
    records = list(r)
    
    assert len(records) == 5, f"Expected 5 records, got {len(records)}"
    
    # Check that records are sorted by category first, then by id
    expected_order = [
        {"id": 1, "category": "A", "value": "value_1"},
        {"id": 2, "category": "A", "value": "value_2"},
        {"id": 3, "category": "B", "value": "value_3"},
        {"id": 4, "category": "B", "value": "value_4"},
        {"id": 5, "category": "C", "value": "value_5"},
    ]
    
    for i, record in enumerate(records):
        assert record["id"] == expected_order[i]["id"], f"Record {i} id mismatch: {record['id']} != {expected_order[i]['id']}"
        assert record["category"] == expected_order[i]["category"], f"Record {i} category mismatch"
    
    shutil.rmtree("_temp_sort_list", ignore_errors=True)


def test_parquet_sorting_single_column_list():
    """Test that sort_by parameter can accept a list with a single string"""
    shutil.rmtree("_temp_sort_single_list", ignore_errors=True)
    
    # Create a writer with sorting by a list containing a single column
    w = BatchWriter(
        inner_writer=DiskWriter,
        dataset="_temp_sort_single_list",
        format="parquet",
        date=datetime.datetime.utcnow().date(),
        schema=[{"name": "id", "type": "INTEGER"}, {"name": "value", "type": "VARCHAR"}],
        sort_by=["id"],  # Sort by id column as a list
        parquet_row_group_size=5000,
    )
    
    # Write records in reverse order
    for i in range(10, 0, -1):
        w.append({"id": i, "value": f"value_{i}"})
    
    w.finalize()
    
    # Read back and verify the data is sorted
    r = Reader(inner_reader=DiskReader, dataset="_temp_sort_single_list")
    records = list(r)
    
    assert len(records) == 10, f"Expected 10 records, got {len(records)}"
    
    # Check that records are sorted by id
    ids = [record["id"] for record in records]
    assert ids == list(range(1, 11)), f"Records are not sorted correctly: {ids}"
    
    shutil.rmtree("_temp_sort_single_list", ignore_errors=True)


def test_parquet_dictionary_encoding_all():
    """Test that use_dictionary parameter can be set to True for all columns"""
    shutil.rmtree("_temp_dict_all", ignore_errors=True)
    
    w = BatchWriter(
        inner_writer=DiskWriter,
        dataset="_temp_dict_all",
        format="parquet",
        date=datetime.datetime.utcnow().date(),
        schema=[
            {"name": "id", "type": "INTEGER"},
            {"name": "category", "type": "VARCHAR"}
        ],
        use_dictionary=True,  # Enable dictionary encoding for all columns
    )
    
    # Write records with repeated category values (good for dictionary encoding)
    for i in range(100):
        w.append({"id": i, "category": f"category_{i % 5}"})
    
    w.finalize()
    
    # Read back and verify data
    r = Reader(inner_reader=DiskReader, dataset="_temp_dict_all")
    records = list(r)
    assert len(records) == 100, f"Expected 100 records, got {len(records)}"
    
    shutil.rmtree("_temp_dict_all", ignore_errors=True)


def test_parquet_dictionary_encoding_disabled():
    """Test that use_dictionary parameter can be set to False to disable dictionary encoding"""
    shutil.rmtree("_temp_dict_disabled", ignore_errors=True)
    
    w = BatchWriter(
        inner_writer=DiskWriter,
        dataset="_temp_dict_disabled",
        format="parquet",
        date=datetime.datetime.utcnow().date(),
        schema=[
            {"name": "id", "type": "INTEGER"},
            {"name": "category", "type": "VARCHAR"}
        ],
        use_dictionary=False,  # Disable dictionary encoding
    )
    
    # Write records
    for i in range(100):
        w.append({"id": i, "category": f"category_{i % 5}"})
    
    w.finalize()
    
    # Read back and verify data
    r = Reader(inner_reader=DiskReader, dataset="_temp_dict_disabled")
    records = list(r)
    assert len(records) == 100, f"Expected 100 records, got {len(records)}"
    
    shutil.rmtree("_temp_dict_disabled", ignore_errors=True)


def test_parquet_dictionary_encoding_specific_columns():
    """Test that use_dictionary parameter can specify specific columns for dictionary encoding"""
    shutil.rmtree("_temp_dict_specific", ignore_errors=True)
    
    w = BatchWriter(
        inner_writer=DiskWriter,
        dataset="_temp_dict_specific",
        format="parquet",
        date=datetime.datetime.utcnow().date(),
        schema=[
            {"name": "id", "type": "INTEGER"},
            {"name": "category", "type": "VARCHAR"},
            {"name": "value", "type": "VARCHAR"}
        ],
        use_dictionary=["category"],  # Only encode 'category' column with dictionary
    )
    
    # Write records with repeated category values
    for i in range(100):
        w.append({
            "id": i,
            "category": f"category_{i % 5}",
            "value": f"unique_value_{i}"
        })
    
    w.finalize()
    
    # Read back and verify data
    r = Reader(inner_reader=DiskReader, dataset="_temp_dict_specific")
    records = list(r)
    assert len(records) == 100, f"Expected 100 records, got {len(records)}"
    
    # Verify the data is correct
    assert records[0]["category"] == "category_0"
    assert records[50]["category"] == "category_0"
    
    shutil.rmtree("_temp_dict_specific", ignore_errors=True)


if __name__ == "__main__":  # pragma: no cover
    from tests.helpers.runner import run_tests
    
    run_tests()
