import time
import datetime
from typing import Any
from dateutil import parser
from .internals.blob_writer import BlobWriter
from ..validator import Schema  # type:ignore
from ...utils import paths
from ...errors import ValidationError, InvalidDataSetError
from ...logging import get_logger
from ...data.formats import json


class SimpleWriter():

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
            dataset: str,
            schema: Schema = None,
            format: str = 'zstd',
            date: Any = None,
            **kwargs):
        """
        Simple Writer provides a basic writer capability.
        """
        if 'BACKOUT' in dataset:
            InvalidDataSetError('BACKOUT is a reserved word and cannot be used in Dataset names')
        if dataset.endswith('/'):
            InvalidDataSetError('Dataset names cannot end with /')
        if '{' in dataset or '}' in dataset:
            InvalidDataSetError('Dataset names cannot contain { or }')
        if '%' in dataset:
            InvalidDataSetError('Dataset names cannot contain %')

        self.schema = schema
        self.finalized = False
        self.batch_date = self._get_writer_date(date)

        self.dataset = dataset
        if "{date" not in self.dataset and not kwargs.get('raw_path', False):
            self.dataset += '/{datefolders}'
        self.dataset = paths.build_path(self.dataset, self.batch_date)

        # add the values to kwargs
        kwargs['raw_path'] = True  # we've just added the dates
        kwargs['format'] = format
        kwargs['dataset'] = self.dataset

        arg_dict = kwargs.copy()
        arg_dict['dataset'] = F'{self.dataset}'
        arg_dict['inner_writer'] = F"{arg_dict.get('inner_writer', {}).__name__}"  # type:ignore
        get_logger().debug(json.serialize(arg_dict))

        # create the writer
        self.blob_writer = BlobWriter(**kwargs)


    def append(self, record: dict = {}):
        """
        Append a new record to the Writer

        Parameters:
            record: dictionary
                The record to append to the Writer

        Returns:
            integer
                The number of records in the current blob
        """
        # Check the new record conforms to the schema before continuing
        if self.schema and not self.schema.validate(subject=record, raise_exception=False):
            raise ValidationError(F'Schema Validation Failed ({self.schema.last_error})')

        self.blob_writer.append(record)

    def __del__(self):
        if hasattr(self, 'finalized') and not self.finalized:
            get_logger().error(F"{type(self).__name__} has not been finalized - data may be lost, make sure you're call .finalize()")

    def finalize(self):
        self.finalized = True
        try:
            self.blob_writer.commit()
        except Exception as e:
            get_logger().error(F"{type(self).__name__} failed to close pool: {type(e).__name__} - {e}")
