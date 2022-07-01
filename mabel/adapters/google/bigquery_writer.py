"""
BigQuery Writer
"""
from enum import Enum
from google.cloud.exceptions import NotFound
from typing import Optional

import os

from mabel.logging.create_logger import get_logger
from mabel.data.writers.internals.base_inner_writer import BaseInnerWriter
from mabel.errors import MissingDependencyError

try:
    from google.cloud import bigquery  # type:ignore

    google_bigquery_installed = True
except ImportError:  # pragma: no cover
    google_bigquery_installed = False

logger = get_logger()

MILLISECONDS_PER_DAY = 86400000


class FieldMode(str, Enum):
    REQUIRED = "REQUIRED"
    NULLABLE = "NULLABLE"


FieldTypeMapping = {
    "VARCHAR": "STRING",
    "NUMERIC": "NUMERIC",
    "BOOLEAN": "BOOLEAN",
    "TIMESTAMP": "TIMESTAMP",
    "STRUCT": None,  # not supported
    "LIST": None,  # not supported
}


class GoogleBigQueryWriter(BaseInnerWriter):
    def __init__(self, project: str, credentials=None, **kwargs):
        if not google_bigquery_installed:  # pragma: no cover
            raise MissingDependencyError(
                "`google-cloud-bigquery` is missing, please install or include in requirements.txt"
            )

        super().__init__(**kwargs)

    def _get_table(
        self, dataset, table, schema, partition_expiration: Optional[int] = None
    ):
        bigquery_client = bigquery.Client()
        dataset = bigquery_client.dataset(dataset)

        # Prepares a reference to the table
        table_ref = dataset.table(table)

        try:
            table = bigquery_client.get_table(table_ref)
        except NotFound:
            # we need to work this out programatically
            schema = [
                bigquery.SchemaField(
                    name="_system_date", field_type="TIMESTAMP", mode=FieldMode.REQUIRED
                ),
                bigquery.SchemaField(
                    name="name", field_type="STRING", mode=FieldMode.REQUIRED
                ),
                bigquery.SchemaField(
                    name="age", field_type="INTEGER", mode=FieldMode.REQUIRED
                ),
            ]
            table = bigquery.Table(table_ref, schema=schema)

            if partition_expiration:
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field="_system_date",
                    expiration_ms=partition_expiration * MILLISECONDS_PER_DAY,
                )

            table = bigquery_client.create_table(table_ref)
            logger.info(
                {
                    "action": "create",
                    "name": table.table_id,
                    "partitioned": (partition_expiration is not None),
                }
            )
        return table

    def commit(self, byte_data, override_blob_name=None):

        self.filename = self.filename_without_bucket

        # if we've been given the filename, use that, otherwise get the
        # name from the path builder
        if override_blob_name:
            blob_name = override_blob_name
        else:
            blob_name = self._build_path()

        try:
            table = self._get_table("users", "justin", None, partition_expiration=90)
        except Exception as err:
            import traceback

            logger = get_logger()
            logger.error(
                f"Error Saving Blob to GCS {type(err).__name__} - {err}\n{traceback.format_exc()}"
            )
            raise err

        return ""
