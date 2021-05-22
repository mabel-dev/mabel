from .base_operator import BaseOperator


class DecoratedOperator(BaseOperator):
    def __init__(self, func=None):
        super().__init__()
        self.name = f"DecoratorOperator:{func.__name__}"
        self.func = func

    def execute(self, data, context):
        response = self.func(data)
        return response, context


def operator(func):
    operator = DecoratedOperator(func=func)
    return operator
