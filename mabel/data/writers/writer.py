import orjson
import datetime
from pydantic import BaseModel  # type:ignore
from typing import Any, Optional, Union, List
from dateutil import parser
from .internals.blob_writer import BlobWriter
from ..validator import Schema
from ...utils import paths
from ...errors import ValidationError, InvalidDataSetError, MissingDependencyError
from ...logging import get_logger


class Writer:
    def _get_writer_date(self, date):
        # default to today if not given a date
        batch_date = datetime.datetime.now()
        if isinstance(date, datetime.date):
            batch_date = date  # type:ignore
        if isinstance(date, str):
            batch_date = parser.parse(date)
        return batch_date

    def __init__(
        self,
        *,
        schema: Optional[Union[Schema, list]] = None,
        set_of_expectations: Optional[list] = None,
        format: str = "zstd",
        date: Any = None,
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

        self.schema = None
        if isinstance(schema, list):
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

        #        if self.schema is not None and self.expectations is not None:
        #            raise InvalidCombinationError(
        #                "Writer does not support having `schema`\`fields` and `expectations` set at the same time."
        #            )

        self.finalized = False
        self.batch_date = self._get_writer_date(date)

        self.dataset_template = dataset
        if "{date" not in self.dataset_template and not kwargs.get("raw_path", False):
            self.dataset_template += "/{datefolders}"
            self.raw_path = True

        self.dataset = paths.build_path(self.dataset_template, self.batch_date)

        # add the values to kwargs
        kwargs["raw_path"] = True  # we've just added the dates
        kwargs["format"] = format
        kwargs["dataset"] = self.dataset

        arg_dict = kwargs.copy()
        arg_dict["dataset"] = f"{self.dataset}"
        arg_dict[
            "inner_writer"
        ] = f"{arg_dict.get('inner_writer', type(None)).__name__}"  # type:ignore
        get_logger().debug(orjson.dumps(arg_dict))

        # default index
        kwargs["index_on"] = kwargs.get("index_on", [])

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
        if hasattr(self, "finalized") and not self.finalized:
            get_logger().error(
                f"{type(self).__name__} has not been finalized - data may be lost, make sure you call .finalize()"
            )

    def finalize(self):
        self.finalized = True
        try:
            return self.blob_writer.commit()
        except Exception as e:
            get_logger().error(
                f"{type(self).__name__} failed to close pool: {type(e).__name__} - {e}"
            )
        return None
