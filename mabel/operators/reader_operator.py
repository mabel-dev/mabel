from ..flows.internals.base_operator import BaseOperator
from ..data import Reader


class ReaderOperator(BaseOperator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.reader = Reader(**kwargs)

    def execute(self, data, context):
        for row in self.reader:
            yield row, context
