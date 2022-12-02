import datetime

from typing import Any, Optional, Union

import orjson

from pydantic import BaseModel

from mabel.data.writers.internals.blob_writer import BlobWriter
from mabel.data.validator import Schema
from mabel.errors import ValidationError, InvalidDataSetError, MissingDependencyError
from mabel.logging import get_logger
from mabel.utils import paths, dates

logger = get_logger()


class Writer:
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
        format: str = "zstd",
        date: Any = None,
        partitions=["year_{yyyy}/month_{mm}/day_{dd}"],
        **kwargs,
    ):
        """
        Simple Writer provides a basic writer capability.
        """
        dataset = kwargs.get("dataset", "")

        if "BACKOUT" in dataset:
            InvalidDataSetError(
                "BACKOUT is a reserved word and cannot be used in Dataset names"
            )
        if dataset.endswith("/"):
            InvalidDataSetError("Dataset names cannot end with /")
        if "{" in dataset or "}" in dataset:
            InvalidDataSetError("Dataset names cannot contain { or }")
        if "%" in dataset:
            InvalidDataSetError("Dataset names cannot contain %")

        # handle transitional states - use the new features to override the legacy features
        if kwargs.get("raw_path") is not None:
            logger.warning(
                "`raw_path` is being deprecated, set `partitions` to `None` instead."
            )
        if str(kwargs.get("raw_path", "")).upper() == "TRUE":
            partitions = None
        if "{date" in dataset:
            logger.warning(
                "settting the date partition in the dataset name is being deprecated, use `partitions` instead."
            )
            if "{datefolders}" in dataset:
                logger.warning("Overriding {datefolders} in dataset name")
                dataset = dataset.replace("{datefolders}", "")
                partitions = ["year_{yyyy}/month_{mm}/day_{dd}"]
            if "{datefolders_short}" in dataset:
                logger.warning("Overriding {datefolders_short} in dataset name")
                dataset = dataset.replace("{datefolders_short}", "")
                partitions = ["{yyyy}/{mm}/{dd}"]

        self.schema = None
        if isinstance(schema, list):
            schema = Schema(schema)
        if isinstance(schema, dict):
            schema = Schema(schema)
        if isinstance(schema, Schema):
            self.schema = schema

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
        self.batch_date = self._get_writer_date(date)

        self.dataset_template = dataset
        self.date_partitions = partitions
        if partitions:
            self.dataset_template += "/" + "/".join(partitions)
            self.date_partitions = None

        self.dataset = paths.build_path(self.dataset_template, self.batch_date)

        # add the values to kwargs
        # kwargs["raw_path"] = True  # we've just added the dates
        kwargs["format"] = format
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
        self.blob_writer = BlobWriter(**kwargs)
        self.records = 0

    def append(self, record: Union[dict, BaseModel]):
        """
        Append a new record to the Writer

        Parameters:
            record: dictionary or pydantic.BaseModel
                The record to append to the Writer

        Returns:
            integer
                The number of records in the current blob
        """
        if isinstance(record, BaseModel):
            record = record.dict()

        if self.schema and not self.schema.validate(
            subject=record, raise_exception=False
        ):
            raise ValidationError(
                f"Schema Validation Failed ({self.schema.last_error})"
            )

        if self.expectations:
            import data_expectations as de  # type: ignore

            de.evaluate_record(self.expectations, record)

        self.blob_writer.append(record)
        self.records += 1

    def __del__(self):
        if hasattr(self, "finalized") and not self.finalized and self.records > 0:
            logger.error(
                f"{type(self).__name__} has not been finalized - {self.records} may have been lost, use `.finalize()` to finalize writers."
            )

    def finalize(self, **kwargs):
        self.finalized = True
        try:
            return self.blob_writer.commit()
        except Exception as e:
            logger.error(
                f"{type(self).__name__} failed to close pool: {type(e).__name__} - {e}"
            )
            raise e
