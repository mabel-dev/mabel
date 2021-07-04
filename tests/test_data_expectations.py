import datetime
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.data.expectations import Expectations
from mabel.data.formats.json import serialize
from mabel.errors import ValidationError
from rich import traceback

traceback.install()

set_of_expectations = [
    {"expectation": "expect_column_to_exist", "column": "string_field"},
    {"expectation": "expect_column_values_to_not_be_null", "column": "string_field"},
    {"expectation": "expect_column_values_to_be_of_type", "column": "boolean_field", "expected_type": "bool"},

]


def test_expectation():

    TEST_DATA = {
        "string_field": "string",
        "integer_field": 100,
        "boolean_field": True,
        "date_field": datetime.datetime.today(),
        "other_field": ["abc"],
        "nullable_field": None,
        "list_field": ["a", "b", "c"],
        "enum_field": "RED",
    }

    test = Expectations(set_of_expectations)
    assert test.test_record(TEST_DATA)




if __name__ == "__main__":  # pragma: no cover
    test_expectation()

    print("okay")
