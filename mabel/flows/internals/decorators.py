from .base_operator import BaseOperator
from ...logging import get_logger


def operatify(func):

    class CustomOperator(BaseOperator):

        def execute(self, data, context):
            response = func(data)
            return response, context

    operator = CustomOperator()
    return operator