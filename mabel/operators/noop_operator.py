"""
No Operation Operator

This does nothing - without error, useful for testing.

It optionally prints the class name - intended to determine when and that
the Operator was run - for testing.
"""
from ..flows.internals.base_operator import BaseOperator


class NoOpOperator(BaseOperator):
    def __init__(self, print_message=False):
        self.print_message = print_message
        super().__init__()

    def execute(self, data={}, context={}):
        if self.print_message:
            print(self.__class__.__name__)
        return data, context
