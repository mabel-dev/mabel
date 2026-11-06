"""
Parquet is written by rugo. These cover the places where mabel has to do work rugo
doesn't: multi-key sorting with nulls, typing all-null columns from the schema, and
holding STRUCT values (rugo has no struct type) as JSON strings.
"""

import datetime
import glob
import os
import shutil
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))

import orjson
import pyarrow.parquet as pq

from mabel.adapters.disk import DiskReader, DiskWriter
from mabel.data import BatchWriter, Reader

SCHEMA = [
    {"name": "a", "type": "INTEGER"},
    {"name": "b", "type": "VARCHAR"},
    {"name": "missing", "type": "TIMESTAMP"},
    {"name": "nested", "type": "STRUCT"},
]

ROWS = [
    {"a": 2, "b": "x", "missing": None, "nested": {"k": 1}},
    {"a": None, "b": "y", "missing": None, "nested": None},
    {"a": 1, "b": "z", "missing": None, "nested": {"k": 2}},
    {"a": 2, "b": "a", "missing": None, "nested": None},
]


def _write(dataset, **kwargs):
    shutil.rmtree(dataset, ignore_errors=True)
    w = BatchWriter(
        inner_writer=DiskWriter,
        dataset=dataset,
        format="parquet",
        date=datetime.date(2026, 1, 1),
        schema=SCHEMA,
        **kwargs,
    )
    for row in ROWS:
        w.append(row)
    w.finalize()
    files = glob.glob(f"{dataset}/**/*.parquet", recursive=True)
    assert len(files) == 1, files
    return pq.ParquetFile(files[0])


def test_parquet_multi_key_sort_nulls_last():
    dataset = "_temp_rugo_sort"
    parquet_file = _write(dataset, sort_by=[("a", "descending"), ("b", "ascending")])
    rows = parquet_file.read().to_pylist()
    assert [(r["a"], r["b"]) for r in rows] == [(2, "a"), (2, "x"), (1, "z"), (None, "y")]

    # the leading key is declared on the file, with nulls last for descending
    sorting = parquet_file.metadata.row_group(0).sorting_columns
    assert len(sorting) == 1
    assert sorting[0].column_index == 0
    assert sorting[0].descending
    assert not sorting[0].nulls_first
    shutil.rmtree(dataset, ignore_errors=True)


def test_parquet_sort_with_nulls_not_declared_ascending():
    # rugo's declaration implies nulls first when ascending, but we put nulls last,
    # so an ascending sort over a column with nulls must not be declared
    dataset = "_temp_rugo_sort_asc"
    parquet_file = _write(dataset, sort_by="a")
    rows = parquet_file.read().to_pylist()
    assert [r["a"] for r in rows] == [1, 2, 2, None]
    assert parquet_file.metadata.row_group(0).sorting_columns == ()
    shutil.rmtree(dataset, ignore_errors=True)


def test_parquet_all_null_column_takes_schema_type():
    dataset = "_temp_rugo_nulls"
    parquet_file = _write(dataset)
    assert str(parquet_file.schema_arrow.field("missing").type) == "timestamp[us]"
    shutil.rmtree(dataset, ignore_errors=True)


def test_parquet_struct_written_as_json():
    dataset = "_temp_rugo_struct"
    _write(dataset)
    day = datetime.date(2026, 1, 1)
    records = list(
        Reader(inner_reader=DiskReader, dataset=dataset, start_date=day, end_date=day)
    )
    nested = [r["nested"] for r in records]
    assert [orjson.loads(n) if n else None for n in nested] == [{"k": 1}, None, {"k": 2}, None]
    shutil.rmtree(dataset, ignore_errors=True)


if __name__ == "__main__":  # pragma: no cover
    from tests.helpers.runner import run_tests

    run_tests()
