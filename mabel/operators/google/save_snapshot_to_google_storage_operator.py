import datetime
from ..internals.base_operator import BaseOperator
from ...data import BatchWriter
from ...adapters.google import GoogleCloudStorageWriter
from ...data.validator import Schema  # type:ignore


class SaveSnapshotToGoogleStorageOperator(BaseOperator):

    def __init__(
            self,
            *,
            project: str,
            dataset: str,
            schema: Schema = None,
            format: str = 'zstd',
            date: datetime.date = None,
            **kwargs):
        """
        This is a wrapper around orwell.data.writers.GoogleCloudStorage.

        Parameters:
            project: string
            dataset: string
            schema: Schema (optional)
            format: string (optional)
            date: date (optional)
        """
        super().__init__()
        self.writer = BatchWriter(
                inner_writer=GoogleCloudStorageWriter,
                project=project,
                dataset=dataset,
                schema=schema,
                format=format,
                date=date,
                **kwargs)

    def execute(self, data: dict = {}, context: dict = {}):
        if data == self.sigterm():
            self.writer.finalize()
            return self.sigterm, context
        else:
            self.writer.append(data)
            return data, context

    def __del__(self):
        self.writer.finalize()
