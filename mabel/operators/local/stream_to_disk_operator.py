import datetime
from ..internals.base_operator import BaseOperator
from ...data import StreamWriter  # type:ignore
from ...adapters.local import FileWriter
from ...data.validator import Schema  # type:ignore


class StreamToDiskOperator(BaseOperator):

    def __init__(
            self,
            *,
            dataset: str,
            schema: Schema = None,
            format: str = 'zstd',
            date: datetime.date = None,
            **kwargs):
        super().__init__()
        self.writer = StreamWriter(
                inner_writer=FileWriter,
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
