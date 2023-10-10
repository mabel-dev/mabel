import datetime
import threading
from typing import Optional
from typing import Union

import orjson
import orso
from orso.logging import get_logger

from mabel.data.validator import Schema
from mabel.errors import MissingDependencyError
from mabel.utils import dates

logger = get_logger()

BATCH_SIZE: int = 100


class DatabaseWriter:
    records = 0

    def _get_writer_date(self, date):
        # default to today if not given a date
        batch_date = datetime.datetime.utcnow()
        if isinstance(date, datetime.date):
            batch_date = date  # type:ignore
        if isinstance(date, str):
            batch_date = dates.parse_iso(date)
        return batch_date

    def __init__(
        self,
        *,
        schema: Optional[Union[Schema, list]] = None,
        set_of_expectations: Optional[list] = None,
        inner_writer=None,
        writer_config: dict = None,
        **kwargs,
    ):
        """
        Simple Writer provides a basic writer capability.
        """
        self.dataset = kwargs.get("dataset", "")

        self.schema = None
        if isinstance(schema, (list, dict)):
            schema = Schema(schema)
        if isinstance(schema, Schema):
            self.schema = schema
        if self.schema is None:
            raise MissingDependencyError("Database writers must have a schema set")
        self.schema = self.schema.schema

        self.expectations = None
        if set_of_expectations:
            try:
                import data_expectations as de  # type: ignore
            except:
                raise MissingDependencyError(
                    "`data_expectations` is missing, please install or include in requirements.txt"
                )
            self.expectations = de.Expectations(set_of_expectations=set_of_expectations)

        self.finalized = False

        kwargs["dataset"] = self.dataset

        arg_dict = kwargs.copy()
        arg_dict["dataset"] = f"{self.dataset}"
        arg_dict[
            "inner_writer"
        ] = f"{arg_dict.get('inner_writer', type(None)).__name__}"  # type:ignore
        logger.debug(orjson.dumps(arg_dict))

        # add the schema to the writer - pyarrow uses this
        # add after the config has been written to the logs
        kwargs["schema"] = self.schema

        # create the writer
        if writer_config is None:
            writer_config = {}
        self.inner_writer = inner_writer(**writer_config)
        self.records = 0

        self.wal = orso.DataFrame(rows=[], schema=self.schema)
        self.records_in_buffer = 0

    def append(self, record: dict):
        """
        Append a new record to the Writer

        Parameters:
            record: dictionary or pydantic.BaseModel
                The record to append to the Writer

        Returns:
            integer
                The number of records in the current blob
        """
        if "BaseModel" in str(type(record)):
            if hasattr(record, "dict"):
                record = record.dict()  # type.ignore
            if hasattr(record, "model_dump"):
                record = record.model_dump()  # type:ignore

        if self.expectations:
            import data_expectations as de  # type: ignore

            de.evaluate_record(self.expectations, record)

        self.wal.append(record)
        self.records_in_buffer += 1
        self.records += 1
        # if this write would exceed the blob size, close it
        if self.wal.rowcount >= BATCH_SIZE:
            self.commit()
            self.wal = orso.DataFrame(rows=[], schema=self.schema)
            self.records_in_buffer = 0

        return self.records_in_buffer

    def __del__(self):
        if hasattr(self, "finalized") and not self.finalized and self.records > 0:
            logger.error(
                f"{type(self).__name__} has not been finalized - {self.records} may have been lost, use `.finalize()` to finalize writers."
            )

    def commit(self):
        if len(self.wal) > 0:
            lock = threading.Lock()

            try:
                lock.acquire(blocking=True, timeout=10)

                self.inner_writer.commit(self.wal)

                get_logger().debug(
                    {
                        "action": "commit",
                        "format": self.inner_writer.__class__.__name__,
                        "records": len(self.wal),
                    }
                )
            finally:
                lock.release()

            self.wal = orso.DataFrame(rows=[], schema=self.schema)
            self.records_in_buffer = 0

    def finalize(self):
        lock = threading.Lock()
        try:
            lock.acquire(blocking=True, timeout=10)

            self.inner_writer.finalize()

            get_logger().debug(
                {"action": "finalize", "format": self.inner_writer.__class__.__name__}
            )
        except Exception as e:
            get_logger().error(
                f"Failed to finalize database write with {self.records} to {self.dataset}"
            )
            raise Exception from e
        finally:
            lock.release()

        return self.records

    def __del__(self):
        # this should never be relied on to save data
        self.commit()
        self.finalize()
