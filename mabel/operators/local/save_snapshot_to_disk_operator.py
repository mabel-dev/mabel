import datetime
from ..internals.base_operator import BaseOperator
from ...data import BatchWriter
from ...adapters.local import FileWriter
from ...data.validator import Schema  # type:ignore



class SaveSnapshotToDiskOperator(BaseOperator):

    def __init__(
            self,
            *,
            dataset: str,
            schema: Schema = None,
            format: str = 'zstd',
            date: datetime.date = None,
            **kwargs):
        super().__init__()
        self.writer = BatchWriter(
                inner_writer=FileWriter,
                dataset=dataset,
                schema=schema,
                format=format,
                date=date,
                **kwargs)

    def execute(self, data: dict = {}, context: dict = {}):
        self.writer.append(data)
        return data, context

    def finalize(self, context):
        self.writer.finalize()
        return context

    def __del__(self):
        self.writer.finalize()
