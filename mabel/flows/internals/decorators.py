from .base_operator import BaseOperator
from ...logging import get_logger
import types


def operatify(func):

    class SimpleOperator(BaseOperator):

        def execute(self, data, context):
            response = func(data)
            if isinstance(response, (list, types.GeneratorType)):
                for item in response:
                    yield item, context
            else:
                return response, context

    operator = SimpleOperator()
    return operator