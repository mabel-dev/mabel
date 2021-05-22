"""
Dictionary Validation Operator

Checks a dictionary against a schema.
"""
from ..flows.internals.base_operator import BaseOperator
from ..data.validator import Schema  # type:ignore
from typing import Any


class ValidatorOperator(BaseOperator):
    def __init__(self, schema: Any):
        self.validator = Schema(schema)
        self.invalid_records = 0
        super().__init__()

    def execute(self, data={}, context={}):
        valid = self.validator(subject=data)
        if not valid:
            self.errors += 1
            return None
        else:
            return data, context
