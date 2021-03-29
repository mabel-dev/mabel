import datetime
from ..internals.base_operator import BaseOperator
from ...data import StreamWriter
from ...adapters.google import GoogleCloudStorageWriter
from ...data.validator import Schema  # type:ignore


class StreamToGoogleStorageOperator(BaseOperator):

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
        This is a wrapper around mabel.data.writers.GoogleCloudStorage.

        Parameters:
            project: string
            dataset: string
            schema: Schema (optional)
            format: string (optional)
            date: date (optional)
        """
        super().__init__()
        self.writer = StreamWriter(
                inner_writer=GoogleCloudStorageWriter,
                project=project,
                dataset=dataset,
                schema=schema,
                format=format,
                date=date,
                **kwargs)

    def execute(self, data: dict = {}, context: dict = {}):
        self.writer.append(data)
        return data, context

    def finalize(self):
        self.writer.finalize()

    def __del__(self):
        try:
            self.writer.finalize()
        except Exception:  # nosec - if this fails, it should be ignored here
            pass
